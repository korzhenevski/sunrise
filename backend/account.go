package backend

import (
	"crypto/sha1"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type Account struct {
	Id        int    `bson:"_id"`
	Email     string `bson:"email"`
	Password  string `bson:"password"`
	CreatedAt int64  `bson:"created_at,minsize"`
}

type AccountService struct {
	db       *mgo.Database
	accounts *mgo.Collection
}

type AccountCreateParams struct {
	Email    string
	Password string
}

type AccountCreateResult struct {
	Id int
}

func passwordHash(password string) string {
	h := sha1.New()
	h.Write([]byte(password))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (a *AccountService) Create(params AccountCreateParams, result *AccountCreateResult) error {
	acc := &Account{
		Email:    params.Email,
		Password: passwordHash(params.Password),
	}

	var err error
	if acc.Id, err = NextId(a.db, "accounts"); err != nil {
		return err
	}

	if err := a.accounts.Insert(acc); err != nil {
		return err
	}

	result.Id = acc.Id
	return nil
}

type AccountAuthParams struct {
	Email    string
	Password string
}

type AccountAuthResult struct {
	Success bool
	Account Account
}

func (a *AccountService) Auth(params AccountAuthParams, result *AccountAuthResult) error {
	where := bson.M{"email": params.Email, "password": passwordHash(params.Password)}
	if err := a.accounts.Find(where).One(&result.Account); err != nil {
		return err
	}

	if result.Account.Id > 0 {
		result.Success = true
	}

	return nil
}

func NewAccountService(db *mgo.Database) *AccountService {
	return &AccountService{db: db, accounts: db.C("users")}
}
