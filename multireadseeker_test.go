package multireadseeker

import (
	"io/ioutil"
	"os"
	"path/filepath"

	. "gopkg.in/check.v1"
)

func (s *MySuite) TestOneReader(c *C) {

	// Create one data file
	dataFile := filepath.Join(s.tmpDir, "data1")
	data := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	err := ioutil.WriteFile(dataFile, data, 0664)
	c.Assert(err, IsNil)

	// Open it
	file, err := os.Open(dataFile)
	c.Assert(err, IsNil)

	mrseeker, err := New(file)
	c.Assert(err, IsNil)
	c.Assert(mrseeker, NotNil)

        // Seek

        // Close
	err = mrseeker.Close()
	c.Assert(err, IsNil)

}
