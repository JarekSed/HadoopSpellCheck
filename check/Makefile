.PHONY: spellcheck test upload

PWD=$(shell pwd)
HADOOP=/.freespace/$(USER)/hadoop/bin/hadoop
TMPDIR=$(shell $(HADOOP) fs -ls /tmp | grep $(USER) | grep tmp_sort_path | sort | tail -n 1 | awk '{print $$8}')
PARTFILES=$(shell $(HADOOP) fs -ls $(TMPDIR) | grep part | awk '{print $$8}')

all: download

download:
	@FILES="$(PARTFILES)"; for FILE in $$FILES; do \
		echo "retrieving $$FILE";										 \
		$(HADOOP) fs -get $$FILE `basename $$FILE`;	 \
	done

cleanup:
	rm -rf part-0000*
