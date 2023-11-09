STACK_DIR := dc_logging_aws
OUTPUT_FOLDER := tests/test_stack_cfn
OUTPUT_FILE := tests/test_stack_cfn/template.yaml

# Recursive wildcard function
rwildcard=$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))

# List of files in the data directory and its subdirectories
DATA_FILES := $(call rwildcard,$(STACK_DIR)/,*)

$(OUTPUT_FOLDER):
	mkdir -p $(OUTPUT_FOLDER)
	echo "*" > $(OUTPUT_FOLDER)/.gitignore

$(OUTPUT_FILE): $(DATA_FILES)
	# Command to generate output.yaml using the data files
	DC_ENVIRONMENT=development LOGS_BUCKET_NAME="dc-monitoring-dev-logging" cdk synth > $(OUTPUT_FILE)


cfn_template_for_tests: $(OUTPUT_FOLDER) $(OUTPUT_FILE)
