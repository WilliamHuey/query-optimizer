
/**
 * Module dependencies.
 */

var Topology = require('tower-topology').Topology;

/**
 * Expose `queryToTopology`.
 */

module.exports = queryToTopology;

/**
 * Convert a `Query` or array of constraints
 * to a `Topology`.
 *
 * XXX: This is going to become more robust.
 *
 * @param {String} ns Adapter namespace
 * @param {Array} constraints
 */

function queryToTopology(ns, constraints) {
  var topology = new Topology
    , name
    , constraint
    // query/create/update/remove
    , action = constraints[constraints.length - 1][1];

  for (var i = 0, n = constraints.length; i < n; i++) {
    constraint = constraints[i];
    switch (constraint[0]) {
      case 'select':
      case 'start':
        topology.stream(name = ns + '.' + action, { constraints: [], collectionName: constraint[1] });
        break;
      case 'constraint':
        // XXX: shouldn't have to create another array here, tmp for now.
        topology.streams[name].constraints.push([ constraint[2], constraint[1], constraint[3] ]);
        break;
      case 'action':
        topology.streams[name].data = constraint[2];
        break;
    }
  }

  return topology;
}