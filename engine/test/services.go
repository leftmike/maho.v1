package test

import (
	"github.com/leftmike/maho/engine/fatlock"
)

type Services struct {
	lockService fatlock.Service
}

func (svcs *Services) Init() {
	svcs.lockService.Init()
}

func (svcs *Services) LockService() fatlock.LockService {
	return &svcs.lockService
}
