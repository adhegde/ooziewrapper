'''Define custom error classes that raise when user inputs an invalid configuration.'''

# Import external dependencies.
import yaml

# Begin error class listings.
class ListError(TypeError):
    '''Raise when user specifies job list without wrapping in a list type.'''

    def __init__(self, input_parameter):

        self.message = "Parameter '" + input_parameter + \
            "' must be a list, even if parameter has only one value."
        super(ListError, self).__init__(self.message)


class InvalidEnvironment(Exception):
    '''Raise when user specifies an invalid environment.'''

    def __init__(self):

        self.message = "Must be 'dev', 'test', or 'prod'."
        super(InvalidEnvironment, self).__init__(self.message)


class KeyException(Exception):
    '''Raise when users enter an invalid job parameter..'''

    def __init__(self, input_job):

        self.input_job = input_job
        self.message = "Invalid key specified in " + input_job['jobName'] + \
            ". Valid job parameters: 'jobType', 'jobName', 'files', 'dependsOn'."
        super(KeyException, self).__init__(self.message)


class RequiredParametersException(Exception):
    '''Raise when users do not correctly specify a set of shared job properties.'''

    def __init__(self):

        self.message = "Please specify a shared set of job properties " + \
            "including workflow name ('name') and resource pool ('queue')."
        super(RequiredParametersException, self).__init__(self.message)


class InvalidJobTypeException(Exception):
    '''Raise when users specify an invalid job type.'''

    def __init__(self):

        self.message = "Valid job types are 'shell', 'spark', 'hive'."
        super(InvalidJobTypeException, self).__init__(self.message)


class FileNotFound(Exception):
    '''Raise when users specify a file dependency that the script cannot find.'''

    def __init__(self, input_file):

        self.pwd = "PWD PLACEHOLDER" # ADD HERE!
        self.message = "Cannot find dependency file " + input_file + ".\n" + \
            "Currently looking in " + pwd
        super(FileNotFound, self).__init__(self.message)


class DependencyError(Exception):
    '''Raise when users specify a job dependency that is not a predefined dictionary.'''

    def __init__(self, input_job, dependency):

        self.message = "Invalid dependency " + dependency + " for job " + \
            input_job['jobName'] + ". Job dependencies must be passed as a " + \
            "list of dictionaries to 'job_list' parameter."
        super(DependencyError, self).__init__(self.message)


class FileRequiredError(Exception):
    '''Raise when users pass an action that requires attached files without files.'''

    def __init__(self, input_job):

        self.message = "A file list is required for " + input_job['jobType'] + " actions."
        super(FileRequiredError, self).__init__(self.message)


# Begin utility functions called by template.py to validate job pieces.
def validate_environment(environment):
    '''Check that valid environment is specified.'''

    if environment not in ['dev','test','prod']:
        raise InvalidEnvironment()


def validate_properties(properties, environment):
    '''Check that all necessary cluster properties are listed.'''
    ## ADD MORE TO DOCSTRING. WHAT PROPERTIES SHOULD BE REQUIRED?

    with open(properties) as f:
        cluster_properties = yaml.safe_load(f)[environment]

    # Check that cluster_properties contains all necessary keys.
    for key in cluster_properties:
        print(cluster_properties)
        break

    return cluster_properties


def validate_keys(job, properties):
    '''Check that job dictionary keys are valid.'''

    for key in job:
        if key not in ['jobType', 'jobName', 'files', 'dependsOn',
                'jobKey', 'queue', 'name', 'email']:
            raise KeyException(job)


def validate_job(job, properties):
    '''Check if all workflow specifications are valid.'''

    # Check that shared job properties contain resource pool and name.
    if 'name' not in properties or 'queue' not in properties:
        raise RequiredParametersException()

    # Check that all workflows specify a valid type.
    if job['jobType'] not in ['shell', 'spark', 'hive']:
        raise InvalidJobTypeException()

    # Check that job dependencies are actually dictionaries.
    if 'dependsOn' in job:
        for dependency in job['dependsOn']:
            if type(dependency) != dict:
                raise DependencyError(job, dependency)

    # Check that job types requiring files have files passed to them.
    if job['jobType'] in ['shell', 'hive'] and 'files' not in job:
        raise FileRequiredError(job)

    # Check that list of files is passed as a list.
    if 'files' in job:
        if type(job['files']) != list:
            raise ListError('files')


    # Check if all workflow files are found locally.
    #for f in job['files']:

    #    # SPECIFY DIRECTORY
    #    if not pathlib.Path(f).is_file():
    #        raise FileNotFound(f)

    # Scrub job names?
    # Definitely make sure that the dependency lists correspond to actual job names.

    # Check that self.job_properties has keys queue and name

    # Check if shell action jobs have sh script for <exec> argument as first argument.
    # Right now, _oozieConfigShell assumes the first element of d['files'] is the
    # desired argument for <exec></exec> just above the file list.

    # Check if hive action jobs have desired sql script as the first file.

    # Check if hive action has queue name specified.
    # Could ask for response prompting users whether they are ok submitting to etl, for example.

    # Check that email is specified if email is required.
