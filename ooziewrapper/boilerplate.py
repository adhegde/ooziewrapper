
def oozieProperties():
    '''Generate boilerplate for properties file.'''

    output_properties = '# Hadoop environment properties\n' + \
        'nameNode=hdfs://nameservice-prod\n' + \
        'resourceManager=yarnRM\n' + \
        'oozie.use.system.libpath=true\n' + \
        'oozie.wf.rerun.failnodes=true\n\n'

    # LONG WAY TO GO HERE.

    return output_properties
