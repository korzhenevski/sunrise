package backend

import (
	"labix.org/v2/mgo"
	// "labix.org/v2/mgo/bson"
)

type RadioService struct {
	db *mgo.Database
}

type GetParams struct {
	SearchQuery string
}

type GetResponse struct {
	Success bool
}

func (r *RadioService) Get(params GetParams, response *GetResponse) error {
	panic("fest")
}

func NewRadio(db *mgo.Database) *RadioService {
	return &RadioService{db: db}
}
