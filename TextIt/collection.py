import json
import requests

#flow_uuid of the flow that you want to collect data for
newFlow = 'a96e05c7-5188-4fbb-9401-73e2eee03028'


def collect(f):

	url = 'https://textit.in/api/v1/runs.json?flow_uuid='+f
	headers = {'Authorization':'Token c9fcc64a4281903cfeb0efa649cae3cc14c459d7'}
	counter = 0

	while url:
		print url
		r = requests.get(url, headers=headers)
		messages = r.json()
		g = messages['results'][0]['created_on'][:10]
		fname = g+'page'+str(counter)+'.json'
		f = open(fname, 'w')
		counter += 1
		f.write(json.dumps(messages, sort_keys=True, indent=4, separators=(',',':')))
		f.close()
		print g
		url = messages['next']


#calls the function 'collect' with the parameter set at the beginning of the script
collect(newFlow)
