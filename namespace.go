package dataset

type Namespace interface {
	Base() string
	Children(Address) (Addresses, error)
	Dataset(Address) (*Dataset, error)
}

type Cursor interface {
	Next() bool
	Close() error
}

type Addresses interface {
	Cursor
	Read() (Address, error)
}
