// Graph pipeline throughput: the REAL `mas_application::workflow_service::
// topological_order` over a 1,000-node layered DAG — the ordering pass the
// API runs on every graph push, and a number we need before claiming the
// deploy-time composition root is fast enough.
//
// Anchored from `crates/application/benches/application_benches.rs`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mas_application::workflow_service::topological_order;
use mas_domain::workflow::WorkflowGraph;
use mas_domain::{WorkflowEdge, WorkflowNode, WorkflowNodeType};

/// 10 layers × 100 nodes, each node linking to two nodes in the next layer.
fn layered_graph() -> WorkflowGraph {
    let layers = 10usize;
    let width = 100usize;
    let mut nodes = Vec::with_capacity(layers * width);
    let mut edges = Vec::with_capacity(layers * width * 2);
    for layer in 0..layers {
        for i in 0..width {
            let key = format!("n{layer}x{i}");
            let node_type = if i == 0 && layer == 0 {
                WorkflowNodeType::Start
            } else if layer + 1 == layers && i == 0 {
                WorkflowNodeType::End
            } else {
                WorkflowNodeType::Agent
            };
            nodes.push(WorkflowNode::new(&key, node_type, &key).expect("node"));
            if layer + 1 < layers {
                for offset in [1usize, 2] {
                    let to = format!("n{}x{}", layer + 1, (i + offset) % width);
                    edges.push(WorkflowEdge::new(&key, &to).expect("edge"));
                }
            }
        }
    }
    WorkflowGraph { nodes, edges }
}

fn bench_kahn_1k(c: &mut Criterion) {
    let graph = layered_graph();
    c.bench_function("application/topological_order_1k_2k", |b| {
        b.iter(|| black_box(topological_order(black_box(&graph)).expect("acyclic")))
    });
}

criterion_group!(benches, bench_kahn_1k);
criterion_main!(benches);
