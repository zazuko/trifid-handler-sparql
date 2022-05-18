import debugLib from 'debug'
import nodeFetch from 'node-fetch'
import SparqlHttpClient from 'sparql-http-client'

const debug = debugLib('trifid:handler-sparql')
SparqlHttpClient.fetch = nodeFetch

function authBasicHeader (user, password) {
  return 'Basic ' + Buffer.from(user + ':' + password).toString('base64')
}

class SparqlHandler {
  constructor (options) {
    this.authentication = options.authentication
    this.resourceNoSlash = options.resourceNoSlash
    this.resourceExistsQuery = options.resourceExistsQuery
    this.resourceGraphQuery = options.resourceGraphQuery
    this.containerExistsQuery = options.containerExistsQuery
    this.containerGraphQuery = options.containerGraphQuery
    this.client = new SparqlHttpClient({ endpointUrl: options.endpointUrl })
  }

  buildQueryOptions () {
    const queryOptions = {}

    if (this.authentication) {
      queryOptions.headers = {
        Authorization: authBasicHeader(this.authentication.user, this.authentication.password)
      }
    }

    return queryOptions
  }

  buildResourceExistsQuery (iri) {
    return this.resourceExistsQuery.split('${iri}').join(iri) // eslint-disable-line no-template-curly-in-string
  }

  buildResourceGraphQuery (iri) {
    return this.resourceGraphQuery.split('${iri}').join(iri) // eslint-disable-line no-template-curly-in-string
  }

  buildContainerExistsQuery (iri) {
    return this.containerExistsQuery.split('${iri}').join(iri) // eslint-disable-line no-template-curly-in-string
  }

  buildContainerGraphQuery (iri) {
    return this.containerGraphQuery.split('${iri}').join(iri) // eslint-disable-line no-template-curly-in-string
  }

  exists (iri, query) {
    debug('SPARQL exists query for IRI <' + iri + '> : ' + query)

    return this.client.selectQuery(query, this.buildQueryOptions()).then(res => {
      if (res.status !== 200) {
        return false
      }

      return res.json()
    }).then(result => {
      return result && result.boolean
    })
  }

  resourceExists (iri) {
    // if resources with trailing slashes are disabled, don't run the query
    if (this.resourceNoSlash && iri.endsWith('/')) {
      return Promise.resolve(false)
    }

    return this.exists(iri, this.buildResourceExistsQuery(iri))
  }

  containerExists (iri) {
    return this.exists(iri, this.buildContainerExistsQuery(iri))
  }

  graphStream (iri, query, accept) {
    debug('SPARQL query for IRI <' + iri + '> : ' + query)

    const queryOptions = this.buildQueryOptions()

    queryOptions.accept = accept

    return this.client.constructQuery(query, queryOptions).then(res => {
      if (res.status !== 200) {
        return null
      }

      const headers = {}

      res.headers.forEach((value, name) => {
        // stream will be decoded by the client -> remove content-encoding header
        if (name === 'content-encoding') {
          return
        }

        headers[name] = value
      })

      return {
        headers: headers,
        stream: res.body
      }
    })
  }

  resourceGraphStream (iri, accept) {
    return this.graphStream(iri, this.buildResourceGraphQuery(iri), accept)
  }

  containerGraphStream (iri, accept) {
    return this.graphStream(iri, this.buildContainerGraphQuery(iri), accept)
  }

  handle (req, res, next) {
    if (req.method === 'GET') {
      this.get(req, res, next, req.iri)
    } else {
      next()
    }
  }

  get (req, res, next, iri) {
    iri = encodeURI(iri)

    debug('handle GET request for IRI <' + iri + '>')

    this.resourceExists(iri).then(exists => {
      if (exists) {
        return this.resourceGraphStream(iri, req.headers.accept)
      }
      if (iri.endsWith('/')) {
        return this.containerExists(iri).then(exists => {
          if (exists) {
            return this.containerGraphStream(iri, req.headers.accept)
          }
          return null
        })
      }
      return null
    }).then(result => {
      if (!result) {
        return next()
      }

      const { headers, stream } = result
      Object.keys(headers).forEach(name => {
        res.setHeader(name, headers[name])
      })

      stream.pipe(res)
    }).catch(next)
  }
}

export default SparqlHandler
