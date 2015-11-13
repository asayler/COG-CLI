# Andy Sayler
# 2014, 2015
# Univerity of Colorado

ECHO = @echo

PYTHON = python3
PIP = pip3

REQUIRMENTS = requirments.txt

UNITTEST_PATTERN = '*_test.py'

.PHONY: all reqs test clean

all:
	$(ECHO) "This is a python project; nothing to build!"

reqs: $(REQUIRMENTS)
	$(PIP) install -r $(REQUIRMENTS) -U

test:
	$(PYTHON) -m unittest discover -v -p $(UNITTEST_PATTERN)

clean:
	$(RM) *.pyc
	$(RM) *~
