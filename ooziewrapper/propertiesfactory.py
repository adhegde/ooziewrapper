'''Generate boilerplate and custom edits for properties file.'''

class Factory(object):

    def __init__(self, run_user, dir_template, cluster_properties, job_properties):

        self.run_user = run_user
        self.dir_template = dir_template
        self.cluster_properties = cluster_properties
        self.job_properties = job_properties

        # Add baseline properties. # DO THESE NEED EDITED?
        self.output_properties = [
            'oozie.use.system.libpath=True',
            'security_enabled=True',
            'dryrun=False', # EDIT THIS PERHAPS?
            'nameNode=hdfs://nameservice-prod',
            'resourceManager=yarnRM',
            'jobName=' + job_properties['name'],
            'jobTracker=yarnRM',
            'userName=' + run_user,
            'hive2_jdbc_url=' + cluster_properties['hive2']['jdbc']['url'],
            'hive2_server_principal=' + cluster_properties['hive2']['server']['principal'],
            'hcat_metastore_uri=' + cluster_properties['hcat']['metastore']['uri'],
            'hcat_metastore_principal=' + cluster_properties['hcat']['metastore']['principal'],
            'edgeNode=' + cluster_properties['edgeNode']
        ]
        # PASSWORD AND QUEUE NOT PRESENT YET.


    def _make_job_properties(self, job, jobs):
        '''Generate job properties for specific job.'''

        # Add workflow directory and check if libpath needs specified.
        this_output = self.output_properties + ['wfDir=' + self.dir_template + job]
        files = jobs[job]['files'] if 'files' in jobs[job] else ''
        this_output += self._add_libs(files)

        # Add workflow application path.
        this_output += ['oozie.wf.application.path=' + self.dir_template + job]

        return '\n'.join(p for p in this_output) + '\n'


    def _add_libs(self, job_files):
        '''Add lib file for jars if necessary.'''

        if any(f.endswith('.jar') for f in job_files):
            return ['oozie.libpath=${nameNode}/user/${userName}/' + \
                'oozie/workspaces/${jobName}/lib']
            # ASSUMING 'lib' DIR IS WHERE OOZIE LOOKS FOR JARS.

        else:
            return []

        # IS THIS PART NECESSARY?
        #output_properties.append('oozie.wf.rerun.failnodes=true')
