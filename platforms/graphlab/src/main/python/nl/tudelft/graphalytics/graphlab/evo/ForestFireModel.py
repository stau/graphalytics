#
# Copyright 2015 Delft University of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import division, print_function
import argparse
import random
import sys
import os
import time

import graphlab as gl
from graphlab.data_structures.sgraph import Edge

import graphlab.deploy.environment
import math


__author__ = 'Jorai Rijsdijk'


def create_environment(hadoop_home, memory_mb, virtual_cores):
    """
    Create a (distributed) Hadoop environment with the given hadoop_home, memory_mb and virtual_cores

    :param hadoop_home: The location of the hadoop_home to get the hadoop config files from hadoop_home/etc/hadoop
    :param memory_mb: The amount of memory to use for processing the algorithm
    :param virtual_cores: The amount of virtual cores to use for graph processing
    :return: The created Hadoop environment object
    """
    return gl.deploy.environment.Hadoop('Hadoop', config_dir=hadoop_home + '/etc/hadoop', memory_mb=memory_mb,
                                        virtual_cores=virtual_cores, gl_source=None)


def parse_args(description, algorithm_name_short, **positional_args):
    """
    Parse the arguments of an algorithm, adding positional arguments specific to the algorithm.

    :param description: The description of the algorithm script
    :param algorithm_name_short: The short name of the algorithm (for graph output filename use)
    :param positional_args: Zero or more keyword arguments, where the keyword is the argument name,
                            and the value is a struct with the keys: type and help to indicate the respective
                            arguments of ArgumentParser.add_argument()
    :return: The result of ArgumentParser.parse_args()
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-t', '--target', default='local', choices=['local', 'hadoop'], required=True,
                        help='Whether to use hadoop or local execution/file storage')
    parser.add_argument('-C', '--cores', default=2, metavar='n', required=False,
                        help='Amount of virtual cores to use, must be at least 2. '
                             'Only used if target is hadoop (Default: 2)')
    parser.add_argument('-H', '--heap-size', default=4096, metavar='n', required=False,
                        help='Amount of memory in MB required for job execution. '
                             'Only used if target is hadoop (Default: 4096)')
    parser.add_argument('--save-result', action='store_true', required=False,
                        help='Save the result graph. Result stored in: target/%s_<graph_file>' % algorithm_name_short)

    parser.add_argument('-f', '--graph-file', metavar='file', required=True, help='The graph file to use')
    parser.add_argument('-d', '--directed', type=bool, required=True,
                        help='Whether or not the input graph is directed.')
    parser.add_argument('-e', '--edge-based', type=bool, required=True,
                        help='Whether or not the input graph is edge based or vertex based.')

    for arg_key in positional_args:
        parser.add_argument(arg_key, type=positional_args[arg_key]['type'], help=positional_args[arg_key]['help'])

    return parser.parse_args()


def save_graph(graph, algorithm_name_short, graph_file):
    """
    Save an SGraph to a file for the testing framework to use.

    :param graph: The graph to save
    :param algorithm_name_short: The short name of the algorithm (used for the filename)
    :param graph_file: The original graph input file path
    """
    graph.save('target/%s_%s' % (algorithm_name_short, graph_file[graph_file.rfind('/', 0, len(graph_file) - 2) + 1:]))


def geometric_random(p):
    if p >= 1.0:
        return 0
    else:
        log_p = math.log(1 - p)
        return int(round(math.log(random.random()) / log_p))


def load_graph_task(task):
    graph_data = gl.SFrame.read_csv(task.params['csv'], header=False, delimiter=' ', column_type_hints=long)
    task.outputs['graph'] = gl.SGraph().add_edges(graph_data, src_field='X1', dst_field='X2')


def select_out_links(graph, vertex, p_ratio, burnt_vertices, new_burning_vertices):
    count = geometric_random(p_ratio)
    possible_neighbours = list()

    # Filter outgoing edges
    for edge in graph.edges.filter_by([vertex], '__src_id'):
        if edge['__dst_id'] not in burnt_vertices and edge['__dst_id'] not in new_burning_vertices:
            possible_neighbours.append(edge['__dst_id'])

    random.shuffle(possible_neighbours)
    new_burning_vertices.update(possible_neighbours[0: min(count, len(possible_neighbours))])


def select_in_links(graph, vertex, r_ratio, burnt_vertices, new_burning_vertices):
    count = geometric_random(r_ratio)
    possible_neighbours = list()

    # Filter outgoing edges
    for edge in graph.edges.filter_by([vertex], '__dst_id'):
        if edge['__src_id'] not in burnt_vertices and edge['__src_id'] not in new_burning_vertices:
            possible_neighbours.append(edge['__src_id'])

    random.shuffle(possible_neighbours)
    # Select either count nodes, or the whole list if it's shorter than that
    new_burning_vertices.update(possible_neighbours[0: min(count, len(possible_neighbours))])


def propagate_fire(graph, new_vertex, ambassador, task):
    burnt_verts = set()
    burnt_verts.add(new_vertex)
    burning_verts = set()
    burning_verts.add(ambassador)

    iteration = 0
    while iteration < task.params['max_iterations'] and len(burning_verts) > 0:
        new_burning_vertices = set()
        for vid in burning_verts:
            select_out_links(graph, vid, task.params['p_ratio'], burnt_verts, new_burning_vertices)
            select_in_links(graph, vid, task.params['r_ratio'], burnt_verts, new_burning_vertices)

        for vid in new_burning_vertices:
            graph = graph.add_edges(Edge(new_vertex, vid))

        burnt_verts.update(new_burning_vertices)
        burning_verts = new_burning_vertices

        iteration += 1

    return graph


def forest_fire_model(task):
    graph = task.inputs['data']
    max_id = task.params['max_id']

    # For each vertex we need to add
    for vid in range(max_id + 1, max_id + 1 + task.params['new_vertices']):
        # Choose an ambassador uniformly random
        ambassador = random.choice(graph.vertices['__id'])
        # Add an edge to the ambassador
        graph = graph.add_edges(Edge(vid, ambassador))
        # Propagate links to the ambassadors neighbours
        graph = propagate_fire(graph, vid, ambassador, task)

    task.outputs['evo_graph'] = graph


def main():
    # Parse arguments
    args = parse_args('Expand a graph using the Forest Fire Model using GraphLab Create', 'evo',
                      max_id={'help': 'The maximum vertex id present in the graph', 'type': long},
                      p_ratio={'help': 'Geometric distribution parameter for the forward burning probability',
                               'type': float},
                      r_ratio={'help': 'Geometric distribution parameter for the backward burning probability',
                               'type': float},
                      max_iterations={'help': 'Maximum number of iterations of the forest fire model to execute',
                                      'type': int},
                      new_vertices={'help': 'The number of new vertices to add to the graph', 'type': int})
    use_hadoop = args.target == "hadoop"

    if not args.edge_based:
        print("Vertex based graph format not supported yet", file=sys.stderr)
        exit(2)

    if use_hadoop:  # Deployed execution
        hadoop_home = os.environ.get('HADOOP_HOME')
        hadoop = create_environment(hadoop_home=hadoop_home, memory_mb=args.heap_size, virtual_cores=args.virtual_cores)

        # Define the graph loading task
        load_graph = gl.deploy.Task('load_graph')
        load_graph.set_params({'csv': args.graph_file, 'directed': args.directed})
        load_graph.set_code(load_graph_task)
        load_graph.set_outputs(['graph'])

        # Define the forest fire model create task
        ff_model = gl.deploy.Task('forest_fire_model')
        ff_model.set_params({'directed': args.directed, 'max_id': args.max_id, 'p_ratio': args.p_ratio,
                             'r_ratio': args.r_ratio, 'max_iterations': args.max_iterations,
                             'new_vertices': args.new_vertices})
        ff_model.set_inputs({'data': ('load_graph', 'graph')})
        ff_model.set_code(forest_fire_model)
        ff_model.set_outputs(['evo_graph'])

        # Create the job and deploy it to the Hadoop cluster
        hadoop_job = gl.deploy.job.create(['load_graph', 'forest_fire_model'], environment=hadoop)
        while hadoop_job.get_status() in ['Pending', 'Running']:
            time.sleep(2)  # sleep for 2s while polling for job to be complete.

        output_graph = ff_model.outputs['evo_graph']
    else:  # Local execution
        # Stub task class
        class Task:
            def __init__(self, **keywords):
                self.__dict__.update(keywords)

        # Stub task object to keep function definitions intact
        cur_task = Task(params={'csv': args.graph_file, 'directed': args.directed, 'max_id': args.max_id,
                                'p_ratio': args.p_ratio, 'r_ratio': args.r_ratio, 'max_iterations': args.max_iterations,
                                'new_vertices': args.new_vertices}, inputs={}, outputs={})

        load_graph_task(cur_task)
        cur_task.inputs['data'] = cur_task.outputs['graph']
        forest_fire_model(cur_task)
        output_graph = cur_task.outputs['evo_graph']

    if args.save_result:
        save_graph(output_graph, 'evo', args.graph_file)


if __name__ == '__main__':
    main()