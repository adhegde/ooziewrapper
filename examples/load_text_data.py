'''Load some text data for our example.'''

import requests
import subprocess

# Write the text of "Pride and Prejudice" to a text file.
pnp_text = 'http://www.kellynch.com/e-texts/Pride%20and%20Prejudice/Pride%20and%20Prejudice,%20Full%20Text.txt'
pnp = requests.get(pnp_text)
with open('pride_and_prejudice.txt', 'w') as f:
    f.write(pnp.content)

# Write the text of "Sense and Sensibility" to a text file.
sas_text = 'http://www.kellynch.com/e-texts/Sense%20and%20Sensibility/Sense%20and%20Sensibility,%20Full%20Text.txt'
sas = requests.get(sas_text)
with open('sense_and_sensibility.txt', 'w') as f:
    f.write(sas.content)

# Put these files on hdfs.
subprocess.call('hdfs dfs -mkdir /tmp/ooziewrapper_example')
subprocess.call('hdfs dfs -put -f pride_and_prejudice.txt' '/tmp/ooziewrapper_example')
subprocess.call('hdfs dfs -put -f sense_and_sensibility.txt' '/tmp/ooziewrapper_example')

# Now we've got some data for our example.
