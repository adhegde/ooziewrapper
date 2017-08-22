'''
A friendly wrapper for oozie. Templating script that creates xml, properties,
and an xml with subworkflows all wrapped together.

THINGS TO TRACK
===============
Are app path and properties adequately passed to subworkflows in xmlfactory?
Figure out if there is more to Hive actions.
Should subprocess.call be changed to subprocess.Popen?
Add error handling to submit method.
How to trackdown logs and output them to a standard location.
Add unit and integration tests.
Build test for changes in yaml file - what to do if yaml file changes and causes a break.
Add more actions to xmlfactory.
Add print_dag method.
===============
'''

# Load base modules.
import collections
import itertools
import os
import pathlib
import subprocess

# Load external dependencies.
import networkx as nx

# Load custom modules.
import propertiesfactory
import validator
import xmlfactory

class OozieWrapper(object):

    def __init__(self, environment, shared_properties, job_list, properties_path, git_repo = None):
        '''
        Take configured jobs as arguments, validate configuration, generate xml output.
        Orchestrate generation of overall workflow with subworkflows if applicable.
        Change oozie parameters based on environment specified, read from yaml file.
        '''

        # Set environment, cluster properties, shared properties, passed jobs,
        # and email flag as instance variables. Cluster properties and environment
        # are validated in custom validation method.
        self.environment = environment
        self.cluster_properties = validator.validate_properties(properties_path, self.environment)
        self.job_properties = shared_properties
        self.jobs = {}
        self.conclusion_email = 'email' in self.job_properties
        self.submitted = False

        # Check that job_list is a list.
        if type(job_list) != list:
            raise validator.ListError('job_list')

        # Check that emails are passed as a list, if applicable.
        if 'email' in self.job_properties and type(self.job_properties['email']) != list:
            raise validator.ListError('shared_properties.email')

        # Git sync on files if a repository is passed.
        # This assumes credentials to the repository are available.
        # May be better to replace this with some sync using the GitHub API.
        self.git_sync(git_repo)

        # Assign each job dictionary a key-value pair in the main jobs dictionary.
        # Also call job validation functions.
        for job in job_list:

            validator.validate_keys(job, self.job_properties)

            self.jobs[job['jobType'] + '-' + str(hash(job['jobName']) % 10000)] = job
            job['jobKey'] = job['jobType'] + '-' + str(hash(job['jobName']) % 10000)
            for prop in self.job_properties:
                job[prop] = self.job_properties[prop]

            validator.validate_job(job, self.job_properties)

        # Instantiate job layout as a graph, with nodes bucketed by job order.
        self._generateDAG()


    def git_sync(self, git_repo):
        '''If applicable, sync remote repository with necessary code files.'''

        # BETTER TO USE GITHUB API OF SOME KIND?

        if git_repo is not None:
            sync = 'git pull ' + git_repo
            subprocess.call(sync.split(' '))


    def submit(self):
        '''
        Put Oozie workflows and scripts on HDFS, extract global workflow name.
        This runs as the user who submits the job. Method also changes "submitted"
        (instance variable for whether job has been submitted) to True if all jobs
        are successfully submitted.
        '''

        # Initate submission variables - directory template, user, workflows, and scripts.
        run_user = subprocess.check_output('whoami', stderr=subprocess.STDOUT)
        run_user = run_user.decode('utf-8').strip('\n')
        dir_template = '/user/' + run_user + '/oozie/workspaces/' + \
            self.job_properties['name'] + '/subworkflows/'

        # Generate propertyFactory and xmlFactory objects, passing cluster and job properties.
        self.propertyFactory = propertiesfactory.Factory(
            run_user,
            dir_template,
            self.cluster_properties,
            self.job_properties
        )
        self.xmlFactory = xmlfactory.Factory(self.job_properties)

        # Generate xml and properties files.
        self.xml = []; self.properties = []
        for job in self.jobs:

            if self.jobs[job]['jobType'] == 'shell':
                self.xml.append((self.jobs[job]['jobKey'], self.xmlFactory._oozieConfigShell(job, self.jobs)))

            elif self.jobs[job]['jobType'] == 'hive':
                self.xml.append((self.jobs[job]['jobKey'], self.xmlFactory._oozieConfigHive(job, self.jobs)))

            self.properties.append((self.jobs[job]['jobKey'], self.propertyFactory._make_job_properties(job, self.jobs)))

        # Generate execution with subworkflows, referencing other workflows.
        self.xml.append(('forked', self._createForks(run_user)))
        self.properties.append(('forked', ''))

        # Write xml to files, in same directory as script is called.
        for workflow, properties in zip(self.xml, self.properties):

            if workflow[0] == 'forked':
                file_name = self.job_properties['name']
            else:
                file_name = workflow[0]

            with open(file_name + '.xml', 'w') as f:
                f.write(workflow[1])
            with open(file_name + '.properties', 'w') as f:
                f.write(properties[1])

        # Initialize hdfs command strings for submission.
        mkdirs = 'hdfs dfs -mkdir -p ' + \
            ' '.join([dir_template + job for job in self.jobs]) + \
            ' ' +  dir_template.replace('subworkflows/', 'main_workflow')

        put_workflows_scripts = []
        for job in self.jobs:
            files_string = ' '.join(self.jobs[job]['files']) if 'files' in self.jobs[job] else ''
            put_workflows_scripts.append('hdfs dfs -put ' + job + '.xml ' + job + '.properties ' + \
                files_string + ' ' + dir_template + job)

        # Do not put main workflow properties on hdfs - wait for run command.
        put_main_workflow = ['hdfs dfs -put ' + self.job_properties['name'] + \
            '.xml ' + dir_template.replace('subworkflows/', 'main_workflow')]

        # Submit each command via subprocess call.
        # ADD ERROR HANDLING!
        for process in [mkdirs] + put_workflows_scripts + put_main_workflow:
            subprocess.call(process.split(' '))

        # Validate oozie workflows through cli call.
        # See here: https://oozie.apache.org/docs/3.1.3-incubating/DG_CommandLineTool.html#Common_CLI_Options

        # THIS MAY ALL BE REPLACED BY API CALLS.

        # Get workflow name. Uses current working directory where main job properties file exists.
        submit_workflow = "oozie job -oozie " + self.cluster_properties['oozie']['url'] + \
            " -config " + self.job_properties['name'] + ".properties -submit"
        to_awk = subprocess.Popen(submit_workflow.split(' '), stdout=subprocess.PIPE)
        get_awk_workflow = ("awk", "-F:", "{print $2}")
        awk_workflow_name = subprocess.check_output(get_awk_workflow, stdin=to_awk.stdout)
        workflow_name = awk_workflow_name.decode('utf-8').strip('\n| ')
        to_awk.wait()

        # Check for success.
        success = True # PLACEHOLDER
        if success:
            self.workflow_name = workflow_name
            self.submitted = True

        # Clean up xml and properties files.
        #for workflow in self.xml:
        #    if workflow[0] == 'forked':
        #        file_name = self.job_properties['name']
        #    else:
        #        file_name = workflow[0]
        #    os.remove(file_name + '.xml')
        #   os.remove(file_name + '.properties')


    def run(self):
        '''Run oozie workflow through oozie CLI'''
        # ADD LOGGING UTILITY.

        if self.submitted:
            start_job = 'oozie job -oozie ' + self.cluster_properties['oozie']['url'] + \
                ' -start ' + self.workflow_name
            subprocess.call(start_job.split(' '))
        else:
            print("Please submit workflow first!") # ADD ERROR HANDLING?

        # oozie job -oozie https://ahlclotxpla115.evv1.ah-isd.net:11443/oozie/ -info 0002160-170601190455443-oozie-oozi-W


    ####
    ### NOT SURE WHAT TO PUT HERE. EDA???
    ###
    @classmethod
    def loaded_run(cls, workflow_name):
        '''Class method to test an already-loaded workflow through oozie CLI.'''
        # ADD LOGGING UTILITY.

        # ADD ERROR HANDLING?
        print('Running ' + workflow_name)

        run_workflow = 'oozie job -oozie ' + self.cluster_properties['oozie']['url'] + \
            ' -start ' + workflow_name

        # ADD HERE


    def _generateDAG(self):
        '''
        Generate workflow DAG using networkx directed graph implementation.
        Return topological ordering of graphs. Note that nx.topological_sort(G)
        requires the graph to be acyclic. Cyclic behavior is hard to implement
        in practice since jobs are defined by calling specific dictionary elements.
        '''

        # Instantiate directed graph, add job dependency edges.
        G = nx.DiGraph()
        for job in self.jobs:
            G.add_node(job)
            if 'dependsOn' in self.jobs[job]:
                for dependency in self.jobs[job]['dependsOn']:
                    G.add_edge(dependency['jobKey'], self.jobs[job]['jobKey'])

        # Sort jobs in graph using topological sort, assigning job buckets.
        # Jobs within the same bucket will be kicked off simultaneously.
        topology = nx.topological_sort(G)
        self.graph = [(0, topology[0])]
        for edge in topology[1:]:
            try:
                self.graph.append((len(nx.shortest_path(G, topology[0], edge)) - 1, edge))
            except nx.exception.NetworkXNoPath as error:
                self.graph.append((0, edge))


    def _createForks(self, run_user):
        '''Generate xml with workflow dependencies by calling subworkflows.'''

        # From self.graph, determine first action (either fork or action depending on
        # number of items in bucket 0) and join prescription.
        bucket_counter = collections.Counter([b for (b, key) in self.graph])
        if bucket_counter[0] > 1:
            first_action = 'fork-0'
        else:
            first_action = [key for (b, key) in self.graph if b == 0][0]

        # Determine list of paths to go to from fork.
        # If there is no fork, call action as top level.
        max_bucket = max(self.graph, key = lambda x: x[0])[0]
        forks = [0] * (max_bucket + 1)

        # Iterate through job buckets.
        for bucket in range(max_bucket + 1):

            # Determine where the next action should go to. If max bucket, go to end.
            if bucket < max_bucket:
                if bucket_counter[bucket + 1] > 1:
                    next_action = 'fork-' + str(bucket + 1)
                else:
                    next_action = 'subworkflow-' + [key for (b, key) in self.graph if b == (bucket + 1)][0]
            else:
                if self.conclusion_email:
                    next_action = "happy-kill"
                else:
                    next_action = "end"

            # If current bucket has greater than 1 job, create fork with boilerplate.
            # Otherwise, create action.
            if bucket_counter[bucket] > 1:
                join_node = 'join-' + str(bucket)
                sub_config = list(itertools.chain(*[
                        self.xmlFactory.boilerplateSubworkflow(run_user, job, self.jobs, join_node) \
                            for (b, job) in self.graph if b == bucket]))
                forks[bucket] = [(2,'<fork name="fork-' + str(bucket) + '">')] + \
                    [(2, '<path start="subworkflow-' + self.jobs[job]['jobKey'] + \
                    '" />') for (b, job) in self.graph if b == bucket] + [ \
                    (2, '</fork>'),
                    (2, '<join name="' + join_node +'" to="' + next_action + '"/>')] + \
                    sub_config
            else:
                relevant_job = [key for (b, key) in self.graph if b == bucket][0]
                forks[bucket] = self.xmlFactory.boilerplateSubworkflow(run_user, relevant_job, self.jobs, next_action)

        # Generate entire xml bundle of workflow forks from bucketed entries in "forks" list.
        forks = list(itertools.chain(*forks))

        # Generate kill configuration, depending on whether an email is specified.
        if self.conclusion_email:
            emails = self.xmlFactory.emailBoilerplate()
            kill_name = 'kill-kill'
        else:
            emails = []
            kill_name = 'kill'
        kill_config = emails + [
            (2, '<kill name="' + kill_name + '">'),
            (4, '<message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>'),
            (2, '</kill>')
        ]

        # Generate configuration for workflow.
        config_list = [
            (0, '<workflow-app name="' + self.job_properties['name'] + '" xlmns="uri:oozie:workflow0.5">'),
            (2, '<start to="' + first_action + '"/>')] + kill_config + forks + [ \
            (2, '<end name="end"/>'),
            (0, '</workflow-app>')
        ]

        string = '\n'.join('\x20' * c[0] + c[1] for c in config_list)
        return string
