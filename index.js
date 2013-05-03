
/**
 * Module dependencies.
 */

var Topology = require('tower-topology').Topology
  , stream = require('tower-stream')
  , adapter = require('tower-adapter')
  // XXX: tmp
  , model = require('tower-model');

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
 * @see http://www.cs.ox.ac.uk/people/dan.olteanu/theses/Robert.Taylor.pdf
 * Compile query to a `Topology`.
 *
 * Builds an acyclic dependency graph.
 *
 * Make sure the graph is **acyclic** (no directed cycles)!
 * @see http://stackoverflow.com/questions/261573/best-algorithm-for-detecting-cycles-in-a-directed-graph
 *
 * @param {String} ns Adapter namespace
 * @param {Array} constraints
 */

function optimize(query, fn) {
  validate(query, function(err){
    if (err) return fn(err);
  });

  // XXX: only support one adapter for now.
  var _adapter = (query.adapters && query.adapters[0]) || 'memory';
  // this.validate();
  // @see http://infolab.stanford.edu/~hyunjung/cs346/ioannidis.pdf
  // var plan = require('tower-query-plan');
  // plan(this).exec()
  // this.validate().plan().exec();
  // optimize(this).exec();
  // 
  // XXX: do validations right here before going to the adapter.
  return adapter(_adapter).exec(query, fn);
}

optimize.topology = exec;
optimize.validate = validate;

/**
 * Add validations to perform before this is executed.
 *
 * XXX: not implemented.
 */

function validate(query, fn) {
  // XXX: only supports one action at a time atm.
  query.errors = [];
  var criteria = query.criteria;
  var action = criteria[criteria.length - 1][1].type;
  var ctx = query;
  // XXX: collect validators for model and for each attribute.
  // var modelValidators = model(criteria[0][1].ns).validators;
  for (var i = 0, n = criteria.length; i < n; i++) {
    if ('constraint' !== criteria[i][0]) continue;

    var constraint = criteria[i][1];
    // XXX: tmp, way to load
    model(constraint.left.ns);

    if (stream.exists(constraint.left.ns + '.' + action)) {
      var _action = stream(constraint.left.ns + '.' + action);//.params;
      var params = _action.params;
      if (params[constraint.left.attr]) {
        // XXX: refactor
        params[constraint.left.attr].validate(ctx, constraint);
        constraint.right.value =
          params[constraint.left.attr].typecast(constraint.right.value);
      }
    }
  }

  // return query.push('validate', fn);
  query.errors.length ? fn(query.errors) : fn();
}

// old
function exec(ns, constraints) {
  var topology = new Topology
    , name
    , constraint
    // query/create/update/remove
    , action = constraints[constraints.length - 1][1].type;

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
        topology.streams[name].data = constraint[1].data;
        break;
    }
  }

  return topology;
}