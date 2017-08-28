UNDER CONSTRUCTION
==================

Things will be in proper order in due time, trust me.

Introducing `ooziewrapper`
--------------------------

I created `ooziewrapper` because I wanted a way to manage configuration of Oozie
workflows as code, and I could not find a module that did things exactly the way
I wanted. So, I wrote my own, and open-sourced it. You are welcome to use it at
your leisure, but I won't take it personal if you don't like it, think it
reinvents the wheel, if somebody already made a better one, etc.

The goal of this module is to be able to create and version Oozie workflows using
Python dictionaries. You can see a full example of the implementation here.

Installation
~~~~~~~~~~~~

pypi: pip install ooziewrapper

git: pip install git+git@github.com:anthonyjgatti/ooziewrapper.git

Context of this module
~~~~~~~~~~~~~~~~~~~~~~

This module has been developed and used in an on-premise Hadoop cluster deployment.
Your mileage may vary with other types of deployments.

Goals of `ooziewrapper` (to name a few)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Wrap job deployment process as part of development.

One common development pattern I observed is as follows:

- Developer writes code in standalone script.
- Developer tinkers with job submission until it works.
- Developer spends lots of time configuring OozieWorkflow.
- Everything falls apart (and / or a code change occurs), start over.

With `ooziewrapper`:
- Developer packages all job artifacts in a directory from the start.
- This includes job resource configuration (critical for Spark).
- No latency between development and deployment (continuous integration, anyone?).

2. Smooth out promotion between environments using "configuration as code".
- Cluster configuration is kept separate from the development process.
- All the developer has to do is change an input from 'test' to 'prod'.
- When updates happen to cluster configuration, the code will not break.
- Makes deploying jobs on multiple clusters much easier.

3. Increase speed of development by removing the UI dependency and allowing seemless
integration of code changes.

Why Wrap Oozie and Not Use Airflow, etc?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Airflow is a completely new scheduling methodology. I'm not that smart.
2. Oozie has a lot of desirable properties and is native to Hadoop.
3. The wrapper allows me to integrate with other services seemlessly, ex. with Tidal. 
If it has an API or CLI, I can work with it.

Dependencies
~~~~~~~~~~~~

This module relies on networkx and PyYAML as the only non-standard Python modules.
The only other requirement is that you run this on a machine that has awk installed.
In general this is intended to be developed and deployed on a Linux machine with
access to the Hadoop cluster.

Specifying Cluster Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



