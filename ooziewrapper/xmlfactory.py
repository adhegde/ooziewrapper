'''
Generate custom xml templates. To be called by template.py. All xml files are
parameterized from corresponding properties file, generated in propertiesfactory.py.
'''

import datetime

class Factory(object):

    def __init__(self, shared_properties):
        '''Initialize object by passing job projects to object.'''

        self.shared_properties = shared_properties


    def _generalBoilerplate(self, job, jobs):
        '''Generates boilerplate xml configuration shared by most individual workflows.'''

        def hive_subinsert(type):
            if type == 'hive':
                return  [
                    (4, '<credential name="hive2" type="hive2">'), # Add HiverServer2 if applicable.
                    (6, '<property>'),
                    (8, '<name>hive2.jdbc.url</name>'),
                    (8, '<value>${hive2_jdbc_url}</value>'),
                    (6, '</property>'),
                    (6, '<property>'),
                    (8, '<name>hive2.server.principal</name>'),
                    (8, '<value>${hive2_server_principal}</value>'),
                    (6, '</property>'),
                    (4, '</credential>')
                ]
            else:
                return []

        return [
            (0,'<workflow-app name="' + jobs[job]['jobName'] + '" xmlns="uri:oozie:workflow:0.5">'),
            (2,'<credentials>'),
            (4,'<credential name="hcat" type="hcat">'),
            (6,'<property>'),
            (8,'<name>hcat.metastore.uri</name>'),
            (8,'<value>${hcat_metastore_uri}</value>'),
            (6,'</property>'),
            (6,'<property>'),
            (8,'<name>hcat.metastore.principal</name>'),
            (8,'<value>${hcat_metastore_principal}</value>'),
            (6,'</property>'),
            (4,'</credential>')] + hive_subinsert(jobs[job]['jobType']) + \
           [(2,'</credentials>'),
            (2,'<start to="' + jobs[job]['jobKey'] + '"/>'), # workflow specific? must match below
            (2,'<kill name="kill">'),
            (4,'<message>Action failed, error ' + \
                'message[${wf:errorMessage(wf:lastErrorNode())}]</message>'),
            (2,'</kill>')]


    def boilerplateSubworkflow(self, run_user, job, jobs, subJoin):
        '''Generate boilerplate code xml configuration common to all sub-workflows.'''

        app_path = '${nameNode}/user/' + str(run_user) + '/oozie/workspaces/' + \
            self.shared_properties['name'] + '/subworkflows/' + jobs[job]['jobKey']
        # xml_name = 'ADD XML NAME HERE' # WE WILL NEED TO INCLUDE AN XML NAME ATTACHED TO THE JOB, I THINK???

        return [
            (2, '<action name="subworkflow-' + jobs[job]['jobKey'] + '">'),
            (4, '<sub-workflow>'),
            (6, '<app-path>' + app_path + '</app-path>'),
            #(6, '<propagate-configuration/>'),
            #(6, '<configuration>'),
            #(8, '<property>'),
            #(10, '<name>subworkflow_' + jobs[job]['jobName'] + '</name>'),
            #(10, '<value>${wfDir}/' + xml_name + '</values>'),
            #(8, '</property>'), # WILL I WANT TO PASS ALL WORKFLOW PROPERTIES?
            # THIS IS WHERE WE WOULD ADD MORE CONFIGURATION FOR SUBWORKFLOW.
            #(6, '</configuration>'),
            (4, '</sub-workflow>'),
            (4, '<ok to="' + subJoin + '"/>'),
            (4, '<error to="kill"/>'),
            (2, '</action>')
        ]


    def _oozieConfigShell(self, job, jobs):
        '''Generate xml for oozie shell actions.'''

        files_to_template = [(6,'<file>${wfDir}/' + f + '#' + \
            f + '</file>') for f in jobs[job]['files']] + \
            [(6,'<file>/user/oozie/share/lib-ext/hive-site.xml#hive-site.xml</file>')] # EDIT IS?

        config_list = self._generalBoilerplate(job, jobs) + [
            (2,'<action name="' + jobs[job]['jobKey'] + '" cred="hcat">'),
            (4,'<shell xmlns="uri:oozie:shell-action:0.1">'),
            (6,'<job-tracker>${jobTracker}</job-tracker>'),
            (6,'<name-node>${nameNode}</name-node>'),
            (6,'<exec>' + jobs[job]['files'][0] + '</exec>')] + \
        files_to_template + [
            (8,'<capture-output/>'),
            (4,'</shell>'),
            (4,'<ok to="end"/>'),
            (4,'<error to="kill"/>'),
            (2,'</action>'),
            (2,'<end Name="end">'),
            (0,'</workflow-app>')
        ]

        string = '\n'.join('\x20' * c[0] + c[1] for c in config_list)
        return string


    def _oozieConfigHive(self, job, jobs):
        '''Generate xml for oozie Hive actions.'''

        config_list = self._generalBoilerplate(job, jobs) + [
            (2,'<action name="' + jobs[job]['jobKey'] + '" cred="hcat">'),
            (4,'<hive xlmns="uri:oozie:hive-action:0.2">'),
            (6,'<job-tracker>${jobTracker}</job-tracker>'),
            (6,'<name-node>${nameNode}</name-node>'),
            (8,'<job-xml>/user/oozie/share/lib-ext/hive-site.xml</job-xml>'), # NOT SURE ABOU THIS?
            (6,'<configuration>'),
            (8,'<property>'),
            (10,'<name>mapred.job.queue.name</name>'),
            (10,'<value>' + jobs[job]['queue'] + '</value>'),
            (8,'</property>'),
            (6,'</configuration>'),
            (6,'<script>${wfDir}/' + jobs[job]['files'][0] + '</script>'), # IS THIS THE RIGHT DIR?
            (4,'</hive>'),
            (4,'<ok to="end"/>'),
            (4,'<error to="kill"/>'),
            (2,'</action>'),
            (0,'</workflow-app>')
        ]

        string = '\n'.join('\x20' * c[0] + c[1] for c in config_list)
        return string


    def emailBoilerplate(self):
        '''Generate boilerplate xml for email actions, to be used in various contexts.'''

        fail_email = [
            (2,'<action name="kill"/>'),
            (4,'<email xlmns="uri:oozie:email-action:0.2">'),
            (6,'<to>' + ','.join([e for e in self.shared_properties['email']]) + '</to>'),
            (6,'<subject>Oozie notification - workflow failed.</subject>'),
            (6,'<body>Oozie workflow ' + self.shared_properties['name'] + \
                ' submitted at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                ' failed with error message [${wf:errorMessage(wf:lastErrorNode())}].</body>'),
            (4,'</email>'),
            (4,'<ok to="kill-kill"/>'),
            (4,'<error to="kill-kill"/>'),
            (2,'</action>')
        ]

        success_email = [
            (2,'<action name="happy-kill"/>'),
            (4,'<email xlmns="uri:oozie:email-action:0.2">'),
            (6,'<to>' + ','.join([e for e in self.shared_properties['email']]) + '</to>'),
            (6,'<subject>Oozie notification - workflow succeeded.</subject>'),
            (6,'<body>Oozie workflow ' + self.shared_properties['name'] + \
                ' submitted at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' succeeded.</body>'),
            (4,'</email>'),
            (4,'<ok to="end"/>'),
            (4,'<error to="kill-kill"/>'),
            (2,'</action>')
        ]

        return fail_email + success_email
