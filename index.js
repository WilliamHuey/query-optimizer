
/**
 * Module dependencies.
 */

var Topology = require('tower-topology').Topology
  , adapter;

/**
 * Expose `optimize`.
 */

module.exports = optimize;

/**
 * Convert a `Query` or array of constraints
 * to a `Topology`.
 *
 * XXX: This is going to become more robust.
 *
 * @param {String} ns Adapter namespace
 * @param {Array} constraints
 */

function optimize(query, fn) {
  return execute(query, fn);
}

var execute = function(query, fn) {
  // lazy load adapter, to prevent dependency cycles.
  adapter = require('tower-adapter');
  execute = exec;
  return exec(query, fn);
}

function exec(query, fn) {
  // XXX: only support one adapter for now.
  var _adapter = query.adapters[0] || 'memory';
  // this.validate();
  // @see http://infolab.stanford.edu/~hyunjung/cs346/ioannidis.pdf
  // var plan = require('tower-query-plan');
  // plan(this).exec()
  // this.validate().plan().exec();
  // optimize(this).exec();
  // 
  // XXX: do validations right here before going to the adapter.
  return adapter(_adapter).exec(query.criteria, fn);
}

// old
function topo(ns, constraints) {
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
        topology.streams[name].constraints.push([ constraint[1].attr, constraint[2], constraint[3] ]);
        break;
      case 'action':
        topology.streams[name].data = constraint[2];
        break;
    }
  }

  return topology;
}