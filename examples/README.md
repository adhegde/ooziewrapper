## Notes on `cluster_properties.yml`.

## Notes on `word_count_workflow.py`.

We've left the example script `word_count_workflow.py` clean without comments so
that it is clearer to the reader what each piece of the script does. In this file
we discuss its contents in more detail.

* This example references Spark code from the following repository:
https://github.com/anthonyjgatti/spark-wordcount-workflow
This repo is cloned into the pwd where you launch the script.

* __Properties file__: For now, it's in the same directory as this example script.
Best practice is to keep this in version control and check out / version as desired
in this script.

* In this example, as specified by the dependencies, job0 and job1 run first, then
job2 and job3 run, then job4 runs. You cannot actually specify a cyclic dependency
that breaks the [DAG specification]() because you must refer to a Python dictionary
that was specified above in the code.

* The 'git_repo' is an optional argument intended to point the remote repository
for your code. Git is currently the only supported version control system.
