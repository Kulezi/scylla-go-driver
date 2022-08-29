package gocql

type Scanner interface {
	// Next advances the row pointer to point at the next row, the row is valid until
	// the next call of Next. It returns true if there is a row which is available to be
	// scanned into with Scan.
	// Next must be called before every call to Scan.
	Next() bool

	// Scan copies the current row's columns into dest. If the length of dest does not equal
	// the number of columns returned in the row an error is returned. If an error is encountered
	// when unmarshalling a column into the value in dest an error is returned and the row is invalidated
	// until the next call to Next.
	// Next must be called before calling Scan, if it is not an error is returned.
	Scan(...interface{}) error

	// Err returns the if there was one during iteration that resulted in iteration being unable to complete.
	// Err will also release resources held by the iterator, the Scanner should not used after being called.
	Err() error
}
