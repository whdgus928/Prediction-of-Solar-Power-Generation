#amount 입력하기
import requests
import json






amounts = [
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 7.569858, 'lower': 7.387664},
 {'upper': 41.842844, 'lower': 34.772371},
 {'upper': 99.405874, 'lower': 83.566753},
 {'upper': 180.338397, 'lower': 153.677837},
 {'upper': 224.089484, 'lower': 187.390066},
 {'upper': 236.284208, 'lower': 210.687178},
 {'upper': 238.729736, 'lower': 223.210609},
 {'upper': 178.578368, 'lower': 173.353717},
 {'upper': 106.176805, 'lower': 95.802024},
 {'upper': 32.119205, 'lower': 28.404067},
 {'upper': 4.022585, 'lower': 3.016834},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0},
 {'upper': 0.0, 'lower': 0.0}]




success = requests.post(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/bids', data=json.dumps(amounts), headers={
                            'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                        }).json()
print(success)