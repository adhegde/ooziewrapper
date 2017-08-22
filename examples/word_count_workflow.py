# TEMPORARY
import sys
sys.path.append('/home/anthony/code/ooziewrapper')


from ooziewrapper.template import OozieWrapper

# List location of cluster properties file.
properties = '/home/anthony/code/ooziewrapper/examples/cluster_properties.yml'

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
    'files': ['hive_transform4.sql'],
    'dependsOn': [job2, job3]
}

test = OozieWrapper(
    environment = 'dev',
    shared_properties = shared,
    job_list = [job0, job1, job2, job3, job4],
    properties_path = properties,
    git_repo = None
    # git_repo = "https://github.com/anthonyjgatti/spark-wordcount-workflow.git"
)

test.submit()
test.run()

# OozieWrapper.loaded_run('test_loaded_workflow_run')
