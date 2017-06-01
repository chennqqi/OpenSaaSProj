import urllib

a = "1EC0uTg5BZW7Y5LhK3nnoi4WzSvjfvB%2BB7Sz4huR%2B%2BSUhfhig2WnLWh%2BSab%2B+KS%2Bfc6%2FkeNq3lnsVYei5kcXlqfVWpoWgFXzGonWVE79vnXXFftLc9iT2Q9Lp+Rno6qz75LTCIJnUZIc%2B27jfVtaNX5hwRwNTWXnbdy18m1d5TbIiqYLDxcKIJ+4N7WktANw%2Ble1Ko9iC8dFSSJPnIJnP2kv5woFXcbisj%2Be5COcCWtb6EK8nGu+p%2BRVbKVt21htDHBSMOcciCQulZnnfFhZkX0rtoqZXdKg14qPoU0MQMqptsJ4+yIy5PCpWo2zQg%2Fx4QpHVStSgd0JtdDnspQEXGa%2FU3bsejePVUAEi1o3OcQco+%2FKo9NUFpAU5mB5mSrGQbNfN5Sc9KIH8vo5tIAQFMO4qDhMg9LSyPwRMmj2RE+CXqbyBwMMnfxD4i%2FLkaDfrAjgw%2B68d0%2BBpqGgW10jrpZdeBlppNXcROTzZz6+VQoacchmnsoM%2BnhLSW0a7q%2BwMf4LvFr%2BSO4DZ20bvLzIeQ3rECXofoaheN1r+uDNHGaJHn4owyaAS0PRaSC%2FEwX3qsiJZWQLK"

b = '1EC0uTg5BZW7Y5LhK3nnoi4WzSvjfvB+B7Sz4huR++SUhfhig2WnLWh+Sab++KS+fc6/keNq3lnsVYei5kcXlqfVWpoWgFXzGonWVE79vnXXFftLc9iT2Q9Lp+Rno6qz75LTCIJnUZIc+27jfVtaNX5hwRwNTWXnbdy18m1d5TbIiqYLDxcKIJ+4N7WktANw+le1Ko9iC8dFSSJPnIJnP2kv5woFXcbisj+e5COcCWtb6EK8nGu+p+RVbKVt21htDHBSMOcciCQulZnnfFhZkX0rtoqZXdKg14qPoU0MQMqptsJ4+yIy5PCpWo2zQg/x4QpHVStSgd0JtdDnspQEXGa/U3bsejePVUAEi1o3OcQco+/Ko9NUFpAU5mB5mSrGQbNfN5Sc9KIH8vo5tIAQFMO4qDhMg9LSyPwRMmj2RE+CXqbyBwMMnfxD4i/LkaDfrAjgw+68d0+BpqGgW10jrpZdeBlppNXcROTzZz6+VQoacchmnsoM+nhLSW0a7q+wMf4LvFr+SO4DZ20bvLzIeQ3rECXofoaheN1r+uDNHGaJHn4owyaAS0PRaSC/EwX3qsiJZWQLK'

c = '1EC0uTg5BZW7Y5LhK3nnoi4WzSvjfvB+B7Sz4huR++SUhfhig2WnLWh+Sab+ KS+fc6/keNq3lnsVYei5kcXlqfVWpoWgFXzGonWVE79vnXXFftLc9iT2Q9Lp Rno6qz75LTCIJnUZIc+27jfVtaNX5hwRwNTWXnbdy18m1d5TbIiqYLDxcKIJ 4N7WktANw+le1Ko9iC8dFSSJPnIJnP2kv5woFXcbisj+e5COcCWtb6EK8nGu p+RVbKVt21htDHBSMOcciCQulZnnfFhZkX0rtoqZXdKg14qPoU0MQMqptsJ4 yIy5PCpWo2zQg/x4QpHVStSgd0JtdDnspQEXGa/U3bsejePVUAEi1o3OcQco /Ko9NUFpAU5mB5mSrGQbNfN5Sc9KIH8vo5tIAQFMO4qDhMg9LSyPwRMmj2RE CXqbyBwMMnfxD4i/LkaDfrAjgw+68d0+BpqGgW10jrpZdeBlppNXcROTzZz6 VQoacchmnsoM+nhLSW0a7q+wMf4LvFr+SO4DZ20bvLzIeQ3rECXofoaheN1r uDNHGaJHn4owyaAS0PRaSC/EwX3qsiJZWQLK'

f = '''{u'jhd_pageName': u'null', u'jhd_netType': u'4g', u'jhd_map': {u'status': u'null', u'1': u'55190eb7e4b08d936ca33cc6', u'0': u'32.101652,118.694448', u'3': False, u'2': u'\u4e13\u79d12014\u7ea7', u'4': u'56ade29b8ac247005398b07c'}, u'jhd_interval': u'17.086803', u'jhd_opTime': u'2016-06-05+23:59:01', u'jhd_opType': u'in', u'jhd_eventId': u'null'}'''

print urllib.quote(c)

print urllib.unquote_plus(a).decode("utf-8")