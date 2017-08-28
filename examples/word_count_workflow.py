# TEMPORARY
import sys
sys.path.append('/home/anthony/code/ooziewrapper')

# Load example data if necessary.
# PEG TO CURRENT DIRECTORY
import os
if not os.path.isfile('pride_and_prejudice.txt'):
    os.system('python load_examples.py')

# Begin example code.
from ooziewrapper.template import OozieWrapper

# List location of cluster properties file.
# Best to keep this under version control.
properties = 'cluster_properties.yml'

# Implement shared properties. This is required and must contain 'name' and 'queue'.
# 'queue' is a resource pool you've defined on your Hadoop cluster.
shared = {
    'name': 'my_project',
    'queue': 'etl',
    'email': ['anthony.j.gatti@gmail.com']
}

# Define jobs.
job0 = {
    'jobType': 'shell',
    'jobName': 'spark_transform_0',
    'files': ['spark_submit_pnp.sh', 'spark_wc_prideandprejudice.py']
}

job1 = {
    'jobType': 'shell',
    'jobName': 'spark_transform_1',
    'files': ['spark_submit_sas.sh', 'spark_wc_senseandsensibility.py']
}

job2 = {
    'jobType': 'hive',
    'jobName': 'hive_transform_2',
    'files': ['pnp_letter_start.sql'],
    'dependsOn': [job0, job1] # This references the Python dictionaries above.
}

job3 = {
    'jobType': 'hive',
    'jobName': 'hive_transform_3',
    'files': ['sas_letter_start.sql'],
    'dependsOn': [job0, job1]
}

job4 = {
    'jobType': 'hive',
    'jobName': 'hive_transform_4',
    'files': ['wc_join.sql'],
    'dependsOn': [job2, job3]
}

test = OozieWrapper(
    environment = 'dev',
    shared_properties = shared,
    job_list = [job0, job1, job2, job3, job4],
    properties_path = properties,
    git_repo = 'https://github.com/anthonyjgatti/spark-wordcount-workflow.git'
)

# git_repo is an optional argument that can be omittied and defaults to none.
# If you are working in the repo locally, it is best to leave this out.
# Generally, you would want this script in a separate repository or at a
# higher directory structure.

test.submit()
test.run()

# OozieWrapper.loaded_run('test_loaded_workflow_run')
