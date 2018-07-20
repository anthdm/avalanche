extern crate byteorder;
extern crate hex;
extern crate rand;
extern crate ring;

use byteorder::{LittleEndian, WriteBytesExt};
use rand::{sample, thread_rng, Rng};
use ring::digest;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{mpsc::{channel, Receiver, Sender},
                Arc,
                Mutex};
use std::thread;

fn main() {
    let mut net = Network::new(10);
    net.run();

    let node = net.nodes.get_mut(&1).unwrap();
    let msg = Message::Transaction(Transaction::random());
    node.lock().unwrap().handle_message(0, &msg);

    loop {}
}

#[derive(Eq, PartialEq, Clone, Hash)]
struct Hash(Vec<u8>);

impl Hash {
    fn to_string(&self) -> String {
        hex::encode(&self.0)
    }
}

impl ::std::fmt::Display for Hash {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", self.to_string())
    }
}

impl ::std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", self.to_string())
    }
}

#[derive(Debug)]
enum Message {
    Query(QueryMessage),
    QueryResponse((u64, QueryResponse)),
    Transaction(Transaction),
}

#[derive(Debug, Clone, PartialEq)]
enum Status {
    Valid,
    Invalid,
}

#[derive(Debug)]
struct QueryResponse {
    hash: Hash,
    status: Status,
}

#[derive(Debug)]
struct QueryMessage {
    tx: Transaction,
    status: Status,
}

#[derive(Debug, Clone)]
struct Transaction {
    nonce: u64,
    /// numbers < 7 are consired valid transactions. Rest is invalid.
    data: i32,
}

impl Transaction {
    fn random() -> Self {
        let mut rng = thread_rng();
        Transaction {
            nonce: rand::random::<u64>(),
            data: rng.gen_range(0, 10),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.write_u64::<LittleEndian>(self.nonce).unwrap();
        buf
    }

    fn hash(&self) -> Hash {
        let digest = digest::digest(&digest::SHA256, &self.serialize());
        Hash(digest.as_ref().to_vec())
    }
}

pub const SAMPLES: usize = 4;
pub const MAX_EPOCHS: u32 = 5;
pub const TRESHOLD: f32 = 0.5;

#[derive(Debug)]
struct Network {
    nodes: HashMap<u64, Arc<Mutex<Node>>>,
    receiver: Arc<Mutex<Receiver<(u64, Message)>>>,
}

impl Network {
    /// Create a new network with `n` participating nodes.
    fn new(n: u64) -> Self {
        let (sender, receiver) = channel();
        Network {
            nodes: (0..n)
                .map(|id| (id, Arc::new(Mutex::new(Node::new(id, sender.clone())))))
                .collect(),
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }

    fn run(&self) {
        let receiver = self.receiver.clone();
        let mut nodes = self.nodes.clone();

        thread::spawn(move || loop {
            let (origin, msg) = receiver.lock().unwrap().recv().unwrap();
            match msg {
                Message::Query(ref _msg) => {
                    let mut sampled = sample_nodes(&nodes, SAMPLES, origin);
                    sampled
                        .iter()
                        .map(|id| {
                            nodes
                                .get_mut(&id)
                                .unwrap()
                                .lock()
                                .unwrap()
                                .handle_message(origin, &msg)
                        })
                        .collect::<Vec<_>>();
                }
                Message::QueryResponse((to, ref _msg)) => {
                    let mut node = nodes.get_mut(&to).unwrap();
                    node.lock().unwrap().handle_message(origin, &msg);
                }
                _ => unreachable!(),
            }
        });
    }
}

fn sample_nodes(nodes: &HashMap<u64, Arc<Mutex<Node>>>, n: usize, excl: u64) -> Vec<u64> {
    let ids: Vec<u64> = nodes
        .iter()
        .filter(|(&id, _)| id != excl)
        .map(|(id, _)| *id)
        .collect();
    sample(&mut thread_rng(), ids, n)
}

#[derive(Debug, Clone)]
struct TxState {
    epoch: u32,
    tx: Transaction,
    status: Status,
    responses: Vec<Status>,
}

impl TxState {
    fn new(tx: Transaction, status: Status) -> Self {
        TxState {
            responses: Vec::new(),
            epoch: 0,
            tx,
            status,
        }
    }

    fn advance(&mut self) {
        self.epoch += 1;
        self.responses.clear();
    }

    fn flip(&mut self) {
        match self.status {
            Status::Valid => self.status = Status::Invalid,
            Status::Invalid => self.status = Status::Valid,
        }
    }
}

#[derive(Debug, Clone)]
struct Node {
    mempool: HashMap<Hash, RefCell<TxState>>,
    id: u64,
    sender: Sender<(u64, Message)>,
}

impl Node {
    fn new(id: u64, sender: Sender<(u64, Message)>) -> Self {
        Node {
            id,
            sender,
            mempool: HashMap::new(),
        }
    }

    fn handle_message(&mut self, origin: u64, msg: &Message) {
        println!("node {} recv from {} => {:?}", self.id, origin, msg);

        match msg {
            Message::Query(ref msg) => self.handle_query(origin, msg),
            Message::QueryResponse((_to, ref msg)) => {
                if let Some((_hash, _status)) = self.handle_query_response(msg) {
                    panic!("got decision");
                };
            }
            Message::Transaction(tx) => self.handle_transaction(tx),
        }
    }

    /// Upon receiving a query, an uncolored node adopts the color in the query,
    /// responds with that color, and initiates its own query, whereas a colored
    /// node simply responds with its current color.
    fn handle_query(&mut self, origin: u64, msg: &QueryMessage) {
        // TODO: This can be so much cleaner, just fighting to much with compiler!!
        let state = if !self.mempool.contains_key(&msg.tx.hash()) {
            let state = TxState::new(msg.tx.clone(), msg.status.clone());
            self.mempool
                .insert(msg.tx.hash(), RefCell::new(state.clone()));
            self.send_query(msg.tx.clone(), msg.status.clone());
            state
        } else {
            let state = self.mempool.get(&msg.tx.hash()).unwrap();
            state.clone().into_inner()
        };
        self.send_response(origin, state.tx.hash(), state.status.clone());
    }

    /// If k responses are not received within a time bound, the node picks an
    /// additional sample from the remaining nodes uniformly at random and queries
    /// them until it collects all responses.     
    /// TODO: timeout + error handling!
    fn handle_query_response(&mut self, msg: &QueryResponse) -> Option<(Hash, Status)> {
        let mut state = self.mempool.get(&msg.hash).unwrap().borrow_mut();
        state.responses.push(msg.status.clone());

        match self.check_responses(&mut state) {
            Some(result) => Some(result),
            None => {
                self.send_query(state.tx.clone(), state.status.clone());
                None
            }
        }
    }

    /// Once the querying node collects k responses, it checks if a
    /// fraction ≥ αk are for the same color, where α > 0.5 is a protocol parameter.
    /// If the αk threshold is met and the sampled color differs from the node’s
    /// own color, the node flips to that color. It then goes back to the query step,
    /// and initiates a subsequent round of query, for a total of m rounds.
    /// Finally, the node decides the color it ended up with at time m.
    fn check_responses(&self, state: &mut TxState) -> Option<(Hash, Status)> {
        if state.responses.len() == SAMPLES {
            let n = state
                .responses
                .iter()
                .filter(|&status| *status == state.status)
                .count();

            if n < (TRESHOLD * SAMPLES as f32) as usize {
                state.flip();
            }

            state.advance();
            if state.epoch == MAX_EPOCHS {
                return Some((state.tx.hash(), state.status.clone()));
            }
        }
        None
    }

    fn handle_transaction(&mut self, tx: &Transaction) {
        // Verify transaction ourself.
        let status = self.verify_transaction(tx);

        // Add the tx to our mempool.
        self.mempool.insert(
            tx.hash(),
            RefCell::new(TxState::new(tx.clone(), status.clone())),
        );
        self.send_query(tx.clone(), status.clone());
    }

    fn send_query(&self, tx: Transaction, status: Status) {
        let msg = Message::Query(QueryMessage {
            tx: tx.clone(),
            status: status.clone(),
        });
        self.sender.send((self.id, msg));
    }

    fn send_response(&self, to: u64, hash: Hash, status: Status) {
        let msg = Message::QueryResponse((to, QueryResponse { hash, status }));
        self.sender.send((self.id, msg));
    }

    fn verify_transaction(&self, tx: &Transaction) -> Status {
        match tx.data < 7 {
            true => Status::Valid,
            false => Status::Invalid,
        }
    }
}
