graph-markdown-files := $(wildcard docs/images/*_template.md)
graph-image-files := $(patsubst %.md,%-1.svg,$(subst _template,,${graph-markdown-files}))

all:graphs

$(graph-image-files): $(graph-markdown-files) 
	./node_modules/.bin/mmdc -i $(patsubst %.svg,%_template.md,$(subst -1,,$@)) -o $(subst -1,,$@) -c docs/mermaid.json

graphs: $(graph-image-files)
