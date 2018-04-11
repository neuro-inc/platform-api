package v1


// TODO: implement status service that stores ID mapping in-memory

type StatusName int

const (
	STATUS_PENDING StatusName = 0
	STATUS_SUCCEEDED StatusName = 1
	STATUS_FAILED StatusName = 2
)

func (name StatusName) String() string {
	names := [...]string {
		"PENDING",
		"SUCCEEDED",
		"FAILED",
	}

	return names[name]
}

type Status struct {
	Id int
	Status StatusName
}

func getStatus(id string) {
}
