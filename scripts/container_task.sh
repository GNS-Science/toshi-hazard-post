# This is the AWS CONTAINER SCRIPT

cd /app
python3 -m ${PYTHON_TASK_MODULE} ${TASK_CONFIG_JSON_QUOTED}
#END_OF_SCRIPT