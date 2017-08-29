## Notes on `cluster_properties.yml`.

See the file `cluster_properties.yml` in this directory for an example of the
structure of the cluster properties file. As of now, __all the properties listed
in the example are required__. I am working on changing this and / or including
a check for required properties in the validation process.

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

* If you use the `git_repo` argument, I would suggest keeping your workflow python
file __in a separate directory__ from your project codebase (for example, having
an `oozie_workflows` repository might be preferable). This will make things a
lot simpler to coordinate in my mind.
