// Copyright (c) 2017 by Gilbert Ramirez <gram@alumni.rice.edu>
package multireadseeker

// Treat a sequence of io.Seeker's as a single io.Seeker.
// In essence, you can treat multiple files as a single, large file.
// It mimics io.MultiReader and io.MultWriter. MultiReadSeeker also
// implements io.Read, io.ReaderAt, and io.Close

import (
	"io"
	//    "os"
	"github.com/crewjam/errset"
	"github.com/pkg/errors"
)

const (
	WHENCE_START   = 0
	WHENCE_CURRENT = 1
	WHENCE_END     = 2
)

// io.ReadSeeker
//        Reader
//        Seeker
// io.Seeker:
//        Seek(offset int64, whence int) (int64, error)
// io.Reader:
//        Read(p []byte) (n int, err error)
// io.ReaderAt:
//        ReadAt(p []byte, off int64) (n int, err error)
// io.Closer
//        Close() error

type ReadCloseSeeker interface {
	io.Reader
	io.Seeker
	io.Closer
}

type MultiReadSeeker struct {
	children []ReadCloseSeeker

	superPosStart []int64
	// The superPos that the last position in the file
	// represents. For a file that has 1 byte, the superPosStart
	// and superPosEnd will be the same.
	superPosEnd []int64

	currentSeekerNum int
	currentSuperPos  int64
}

// Allocate and initialize a new MultiReadSeeker
func New(children ...ReadCloseSeeker) (*MultiReadSeeker, error) {
	mrseeker := &MultiReadSeeker{}
	err := mrseeker.Initialize(children...)
	if err != nil {
		return nil, err
	}
	return mrseeker, nil
}

// Initialize a newly-allocated MultiReadSeeker
func (self *MultiReadSeeker) Initialize(children ...ReadCloseSeeker) error {
	if len(self.children) != 0 {
		panic("MultiReadSeeker already initialized")
	}
	if len(children) == 0 {
		panic("MultiReadSeeker needs at least one child")
	}

	self.children = make([]ReadCloseSeeker, len(children))
	self.superPosStart = make([]int64, len(children))
	self.superPosEnd = make([]int64, len(children))

	for i, child := range children {
		self.children[i] = child
		// Go to the end of the seeker
		endPos, err := child.Seek(0, WHENCE_END)
		if err != nil {
			return errors.Wrapf(err, "Seeking to end of %v", child)
		}
		self.superPosEnd[i] = endPos
		// This file starts after the previous file ends
		if i > 0 {
			self.superPosStart[i] = self.superPosEnd[i-1] + 1
		}
		// Reposition to the beginning
		_, err = child.Seek(0, WHENCE_START)
		if err != nil {
			return errors.Wrapf(err, "Seeking to start of %v", child)
		}
	}
	return nil
}

func (self *MultiReadSeeker) Close() error {
	errs := errset.ErrSet{}
	for i, child := range self.children {
		err := child.Close()
		if err != nil {
			errs = append(errs,
				errors.Wrapf(err, "Closing io.Seeker #%d (0-based)", i))
		}
	}
	// nil if there were no errors
	return errs.ReturnValue()
}

/*

type ConcatFile struct {
    // name given to Open() by caller
//    public_name         string

    // Is the ConcatFile currently open?
    // We mark it as closed after an unrecoverable error
    currently_open      bool

    // All the filenames that make up the complete data set.
    filenames           []string

    // Number of files (length of 'filenames')
    num_files           int

    // The currently open file number
    current_file_num    int

    // The currently opened filehandle, and the position within it
    fh                  *os.File

    // Our position (across the sequence of all files)
    super_pos           int64

    // Our complete size (across the sequence of all files)
    super_size          int64

    // Position/size info for all the files
    super_pos_start     []int64
    super_pos_end       []int64
}

//
// Open returns a new ConcatFile, similar to os.Open(), except
// that the input is a slice of file names, as opposed to just one.
//
func Open(names []string) (self *ConcatFile, err error)  {
    // Ensure we have at least one file
    if len(names) == 0 {
        return nil, errors.New("At least one file name is required")
    }

    // Create the new object and analyze the files to
    // fill in the object data
    self = new(ConcatFile)

    // Fill in initial data
    self.filenames = names
    self.num_files = len(names)
    self.current_file_num = 0
    self.super_pos_start = make([]int64, self.num_files)
    self.super_pos_end = make([]int64, self.num_files)
    self.super_pos_start[0] = 0

    // Go through each file and find its size
    for i := 0 ; i < self.num_files ; i++ {

        // Any permission problems?
        fh, err := os.Open(names[i])
        if err != nil {
            return nil, err
        }

        // This file starts after the previous file ends
        if i > 0 {
            self.super_pos_start[i] = self.super_pos_end[i - 1] + 1
        }

        // stat the file
        fileinfo, err := fh.Stat()
        fh.Close() // ignore any error
        if err != nil {
            return nil, err
        }

        // Mark the end of this file
        self.super_pos_end[i] = self.super_pos_start[i] + fileinfo.Size() - 1
    }
    // Our super-size
    self.super_size = self.super_pos_end[self.num_files - 1] + 1

    // Open the first file
    self.fh, err = os.Open(names[0])
    if err != nil {
        return nil, err
    }
//    self.fh_pos = 0
    self.currently_open = true

    return self, nil
}

//
// Close() a ConcatFile
//
func (self *ConcatFile) Close() (err error)  {
    if self.currently_open {
        self.currently_open = false
        return self.fh.Close()
    } else {
        return nil
    }
}

//
// Read len(b) bytes
// See:
// http://golang.org/pkg/io/#Reader
//
func (self *ConcatFile) Read(b []byte) (n int, err error) {

    if self.currently_open == false {
        return 0, io.ErrUnexpectedEOF // XXX what should I really return here?
    }
    num_requested := len(b)

    n, err = self.fh.Read(b)
    self.super_pos += int64(n)

    // Did we read the exact amount?
    if n == num_requested {
        return n, err
    }

    // Don't bother checking n > num_requested; it's impossible.

    // We got less than num_requested.  Is there another file to go to?
    incremented, incr_err := self.goToNextFile()
    if incr_err != nil {
        // Had a problem opening the next file. Return an error
        return n, incr_err
    }

    if incremented {
        // Read some more from the next file
        sub_n, sub_err := self.Read(b[n:])
        return n + sub_n, sub_err

    } else if n > 0 {
        // No other file. Return what we got.
        return n, err

    } else {
        // We read nothing and there's no other file. Return EOF
        return n, io.EOF
    }

}

//
// Close the current file and go to the next file if possible.
// Returns (true, err), or (false, nil)
//
func (self *ConcatFile) goToNextFile() (success bool, err error) {

    // Any more files?
    if self.current_file_num < self.num_files - 1 {
        // Close the current file
        self.fh.Close()

        // Open the next file
        self.current_file_num++
        self.fh, err = os.Open(self.filenames[self.current_file_num])
        return true, err
    } else {
        return false, nil
    }
}


const WHENCE_ORIGIN int = 0
const WHENCE_CURRENT int = 1
const WHENCE_END int = 2

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence:
//  0 means relative to the origin of the file,
//  1 means relative to the current offset, and
//  2 means relative to the end.
// It returns the new offset and an error, if any.
func (self *ConcatFile) Seek(offset int64, whence int) (ret int64, err error) {

    var new_super_pos int64
    var seek_index int

    // Calculate the new super position
    if whence == WHENCE_ORIGIN {
        new_super_pos = offset
        // Which file will that be in?
        if new_super_pos == 0 {
            seek_index = 0
        } else {
            seek_index = self.findSeekIndex(new_super_pos)
        }

    } else if whence == WHENCE_CURRENT {
        // Special case: no change
        if offset == 0 {
            return self.super_pos, nil
        }
        new_super_pos = self.super_pos + offset

        // Which file will that be in?
        seek_index = self.findSeekIndex(new_super_pos)

    } else if whence == WHENCE_END {
        // offset must be 0 or negative
        if offset > 0 {
            return self.super_pos, errors.New("Seek(offset, WHENCE_END); seek must be <= 0")
        }
        new_super_pos = self.super_size + offset

        // Which file will that be in?
        seek_index = self.findSeekIndex(new_super_pos)

    } else {
        return self.super_pos, errors.New("Seek(offset, whence); whence must be 0, 1, or 2")
    }

    // If it's beyond our max limit, go to the last file, unless
    // we were going backwards (WHENCE_END); in that case, go to the
    // first file
    if seek_index == seekImpossible {
        if whence == WHENCE_END {
            seek_index = 0
        } else {
            seek_index = self.num_files - 1
        }
    }

    // Do we need to change files?
    if seek_index != self.current_file_num {
        // Close the current one
        err = self.fh.Close()
        if err != nil {
            self.currently_open = false
            return self.super_pos, err
        }

        // Open the correct one
        self.fh, err = os.Open(self.filenames[seek_index])
        if err != nil {
            self.currently_open = false
            return self.super_pos, err
        }
        self.current_file_num = seek_index
    }

    // Seek to the absolute position in the correct file
    offset = new_super_pos - self.super_pos_start[seek_index]
    this_file_offset, err := self.fh.Seek(offset, WHENCE_ORIGIN)
    if err != nil {
        self.currently_open = false
        self.super_pos = self.super_pos_start[seek_index] + this_file_offset
        return self.super_pos, err
    }
    self.super_pos = new_super_pos
    return self.super_pos, nil
}


const seekImpossible int = -1
//
// Given a super position, return the index of the file where that
// index will be located. If we have no such file, return seekImpossible (-1)
//
func (self *ConcatFile) findSeekIndex(new_super_pos int64) (index int) {

    // The super position must not be negative
    if new_super_pos < 0 {
        return seekImpossible
    }

    // Go through each file and examine the boundaries
    for i := 0 ; i < self.num_files ; i++ {
        if new_super_pos >= self.super_pos_start[i] &&
            new_super_pos <= self.super_pos_end[i] {
            return i
        }
    }

    // Beyond the end?
    return seekImpossible
}

//
// Technically this should check for self.currently_open,
// be we already know the position and we don't have to
// access the underlying filehandle, so just return the value.
func (self *ConcatFile) Tell() (int64) {
    return self.super_pos
}

func (self *ConcatFile) Size() (int64) {
    return self.super_size
}
*/
