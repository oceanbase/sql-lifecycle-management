#!/bin/sh

set -euf

black .
ruff check --fix .