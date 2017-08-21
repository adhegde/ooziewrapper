# TEMPORARY
import sys
sys.path.append('/home/anthony/code/ooziewrapper')

from ooziewrapper.template import OozieWrapper

# List location of cluster properties file. For now, it's in the same directory
# as this example script. Best practice is to keep this in version control and
# check out / version as desired in this script.
properties = '~/code/ooziewrapper/examples/cluster_properties.yml'

# Implement shared properties. This is required and must contain 'name' and 'queue'.
# 'queue' is a resource pool you've defined on your Hadoop cluster.
shared = {
    'name': 'my_project',
    'queue': 'etl',
    'email': ['anthony.j.gatti@gmail.com']
}

# Define jobs.

# In this example, as specified by the dependencies, job0 and job1 run first,
# then job2 and job3 run, then job4 runs. You cannot actually specify a cyclic
# dependency that breaks the DAG specification because you must refer to a
# Python dictionary that was specified above in the code.

job0 = {
    'jobType': 'shell',
    'jobName': 'spark_transform_0',
    'files': ['spark_submit_0.sh', 'spark_transform_0.py']
}

job1 = {
    'jobType': 'shell',
    'jobName': 'spark_transform_1',
    'files': ['spark_submit_1.sh', 'spark_transform_1.py']
}

job2 = {
    'jobType': 'hive',
    'jobName': 'hive_transform_2',
    'files': ['hive_transform_2.sql'],
    'dependsOn': [job0, job1] # This references the Python dictionaries above.
}

job3 = {
    'jobType': 'hive',
    'jobName': 'hive_transform_3',
    'files': ['hive_transform_3.sql'],
    'dependsOn': [job0, job1]
}

job4 = {
    'jobType': 'hive',
    'jobName': 'hive_transform_4',
    'files': ['hive_transform_4.sql'],
    'dependsOn': [job2, job3]
}

test = OozieWrapper(
    environment = 'dev',
    shared_properties = shared,
    job_list = [job0, job1, job2, job3, job4],
    properties_path = properties,
    git_repo = "git@github.com:organization/my_project.git" # Optional parameter defaulting to None.
) # This arugment is intended to point the remote repository for your code. Git is currently the only
# supported version control software.

test.submit()
test.run()

# OozieWrapper.loaded_run('test_loaded_workflow_run')
