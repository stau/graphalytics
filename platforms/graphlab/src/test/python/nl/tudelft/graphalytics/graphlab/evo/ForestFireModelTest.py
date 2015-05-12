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
import graphlab as gl
import sys

__author__ = 'Jorai Rijsdijk'


def parse_args(description):
    """
    Parse the arguments of an algorithm test.

    :param description: The description of the algorithm script
    :return: The result of ArgumentParser.parse_args()
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("graph_name", help="The name of the result graph to load (relative path)")
    parser.add_argument("original_input", help="The file with the original input of the algorithm")
    parser.add_argument("first_new_vertex", help="The first new vertex that was created", type=long)
    parser.add_argument("vertices_added", help="The amount of new vertices that should have been added", type=int)
    parser.add_argument("max_iterations", help="The maximum amount of iterations of the algorithm executed", type=int)
    return parser.parse_args()


def float_not_equals(expected, actual, delta=0.000001):
    return expected - delta > actual or expected + delta < actual


def verify_original_graph_unchanged(original_graph, result_graph):
    if len(original_graph.vertices.join(result_graph.vertices)) != len(original_graph.vertices):
        print("Not all original vertices still exist in the result graph", file=sys.stderr)
        exit(1)

    for node in original_graph.vertices['__id']:
        original_edges = original_graph.get_neighborhood([node], full_subgraph=False)
        result_edges = result_graph.get_neighborhood([node], full_subgraph=False)

        if len(original_edges.edges) != 0 \
                and len(original_edges.edges.join(result_edges.edges)) != len(original_edges.edges):
            print("Not all original edges for vertex %d still exist in the result graph" % node, file=sys.stderr)
            exit(1)


def verify_new_vertices_created(original_graph, result_graph, first_new_vertex, vertices_added):
    vertices_actually_added = len(result_graph.vertices) - len(original_graph.vertices)
    if vertices_actually_added != vertices_added:
        print("The amount of vertices actually added doesn't match the expected amount. (Expected: %d, Actual: %d)"
              % (vertices_added, vertices_actually_added), file=sys.stderr)
        exit(1)

    i = first_new_vertex
    while i < first_new_vertex + vertices_added:
        if len(original_graph.vertices[original_graph.vertices['__id'] == i]) == 1:
            print("Vertex %d that should have been added was already present in the original graph." % i,
                  file=sys.stderr)
            exit(1)

        if len(result_graph.vertices[result_graph.vertices['__id'] == i]) == 0:
            print("Vertex %d that should have been added was not present in result graph." % i, file=sys.stderr)
            exit(1)

        i += 1


def verify_new_edges_feasible(original_graph, result_graph, added_vertex, number_of_iterations):
    neighbors = result_graph.get_neighborhood([added_vertex], full_subgraph=False)
    for edge in neighbors.edges:
        dst_edge = edge['__dst_id']
        if edge['__src_id'] == added_vertex and added_vertex <= edge['__dst_id']:
            print("New vertex %d formed an edge to vertex %d, which is newer." % (added_vertex, dst_edge),
                  file=sys.stderr)
            exit(1)

    if len(neighbors.edges) == 0:
        print("New vertex %d is not connected to any vertex." % added_vertex, file=sys.stderr)
        exit(1)

    # Get the sub_graph
    sub_graph = result_graph.get_neighborhood([added_vertex], radius=number_of_iterations + 1, full_subgraph=True)
    valid = False
    for node in sub_graph.vertices:
        if node['__id'] != added_vertex:
            valid = valid or max_distance_within_limit(sub_graph, node['__id'], number_of_iterations, added_vertex)

    if not valid:
        print("Connected vertices for new vertex %d aren't within maximum distance of potential source" % added_vertex,
              file=sys.stderr)
        exit(1)


def max_distance_within_limit(graph, source, limit, ignore):
    graph = gl.shortest_path.create(graph, source_vid=source)
    output = graph['distance'].apply(lambda row: row['distance'] > limit and row['__id'] != ignore)
    # If the amount of nodes that aren't reachable within limit steps from this potential ambassador
    # (ignoring the new vertex) is 0, this source node would be a valid ambassador node.
    return output.sum() == 0


def main():
    args = parse_args('Test the result of the CommunityDetection algorithm')
    result_graph = gl.load_sgraph(args.graph_name)
    graph_data = gl.SFrame.read_csv(args.original_input, header=False, delimiter=' ', column_type_hints=long)
    original_graph = gl.SGraph().add_edges(graph_data, src_field='X1', dst_field='X2')
    first_new_vertex = args.first_new_vertex
    vertices_added = args.vertices_added
    number_of_iterations = args.max_iterations

    # Verify that the original graph is unchanged
    print("Verify original graph unchanged")
    verify_original_graph_unchanged(original_graph, result_graph)

    # Verify that new vertices are created with expected id's
    print("Verify that new vertices are created")
    verify_new_vertices_created(original_graph, result_graph, first_new_vertex, vertices_added)

    # Verify that the newly added edges are feasible
    print("Verify that new edges are feasible")
    i = first_new_vertex
    while i < first_new_vertex + vertices_added:
        print("Verifying for vertex %d" % i)
        verify_new_edges_feasible(original_graph, result_graph, i, number_of_iterations)

        i += 1

if __name__ == '__main__':
    main()
