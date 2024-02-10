vendor = vonmo
project = infra
app = rocker

DOCKER = $(shell which docker)
ifeq ($(DOCKER),)
$(error "Docker not available on this system")
endif

DOCKER_CMD = $(shell which docker)
DOCKER_COMPOSE_OLD_CMD = $(shell which docker-compose)
DOCKER_COMPOSE_NEW_CMD = $(shell which docker)
ifeq ($(DOCKER_COMPOSE_NEW_CMD),)
  ifeq ($(DOCKER_COMPOSE_OLD_CMD),)
    $(error "DockerCompose not available on this system")
  else
    DOCKER_COMPOSE_CMD = ${DOCKER_COMPOSE_OLD_CMD}
  endif
else
  DOCKER_COMPOSE_CMD = ${DOCKER_COMPOSE_NEW_CMD} compose
endif

compose-uid := $(shell echo $(vendor)-$(app)-$(notdir $(shell pwd)) | tr A-Z a-z)
compose-prefix = -p ${compose-uid}

DOCKER_COMPOSE=${DOCKER_COMPOSE_CMD} ${compose-prefix}

# use to override vars for your platform
ifeq (env.mk,$(wildcard env.mk))
	include env.mk
endif

.PHONY: test

all: build_imgs up test rel

build_imgs:
	@echo "Update docker images..."
	@${DOCKER_COMPOSE} build

up:
	@${DOCKER_COMPOSE} up -d

down:
	@${DOCKER_COMPOSE} down

compile:
	@${DOCKER_COMPOSE} exec test bash -c "cd /project && make -f sandbox.mk compile"

test:
	@echo "Testing..."
	@${DOCKER_COMPOSE} exec test bash -c "cd /project && make -f sandbox.mk tests"

rel:
	@echo "Build release..."
	@${DOCKER_COMPOSE} exec test bash -c "cd /project && make -f sandbox.mk prod"

lint:
	@echo "Lint..."
	@${DOCKER_COMPOSE} exec test bash -c "cd /project && make -f sandbox.mk lint"

xref:
	@echo "Xref analysis..."
	@${DOCKER_COMPOSE} exec test bash -c "cd /project && make -f sandbox.mk xref"

dialyzer:
	@echo "Dialyzer..."
	@${DOCKER_COMPOSE} exec test bash -c "cd /project && make -f sandbox.mk dialyzer"
