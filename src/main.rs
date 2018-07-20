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

    loop {
        let tx = Transaction::random();
        println!("sending new transaction into the network {}", &tx.hash());

        // Pick a random node in the network let the node handle the random transaction.
        // All transactions with a number < 7 are considered invalid.
        let id = thread_rng().gen_range(0, net.nodes.len()) as u64;
        let node = net.nodes.get_mut(&id).unwrap();
        node.lock()
            .unwrap()
            .handle_message(0, &Message::Transaction(tx));

        thread::sleep_ms(500); // cpu ded
    }
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

impl Status {
    fn flip(&self) -> Status {
        match self {
            Status::Valid => Status::Invalid,
            Status::Invalid => Status::Valid,
        }
    }
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
pub const MAX_EPOCHS: u32 = 4;
pub const TRESHOLD: f32 = 0.75;
pub const CONVICTION_TRESHOLD: f32 = 0.75;

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
    is_final: bool,

    /// 1. Each node maintains a counter cnt
    /// 2. Upon every color change, the node resets cnt to 0
    /// 3. Upon every successful query that yields ≥ αk responses for the same
    /// color as the node, the node increments cnt.
    cnt_valid: u32,
    cnt_invalid: u32,
    cnt: u32,

    /// Last decided status.
    last_status: Status,
}

impl TxState {
    fn new(tx: Transaction, status: Status) -> Self {
        TxState {
            responses: Vec::new(),
            is_final: false,
            last_status: Status::Invalid,
            epoch: 0,
            cnt_valid: 0,
            cnt_invalid: 0,
            cnt: 0,
            tx,
            status,
        }
    }

    fn incr_status(&mut self, s: &Status) -> u32 {
        match s {
            Status::Valid => {
                self.cnt_valid += 1;
                self.cnt_valid
            }
            Status::Invalid => {
                self.cnt_invalid += 1;
                self.cnt_invalid
            }
        }
    }

    fn status_count(&self, s: &Status) -> u32 {
        match s {
            Status::Valid => self.cnt_valid,
            Status::Invalid => self.cnt_invalid,
        }
    }

    fn advance(&mut self) {
        self.epoch += 1;
        self.responses.clear();
    }
}

#[derive(Debug, Clone)]
struct Node {
    mempool: HashMap<Hash, TxState>,
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
        //println!("node {} recv from {} => {:?}", self.id, origin, msg);

        match msg {
            Message::Query(ref msg) => self.handle_query(origin, msg),
            Message::QueryResponse((_to, ref msg)) => {
                if let Some((hash, status)) = self.handle_query_response(msg) {
                    println!("node {} got decision {:?} for tx {}", self.id, status, hash);
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
            self.mempool.insert(msg.tx.hash(), state.clone());
            self.send_query(msg.tx.clone(), msg.status.clone());
            state
        } else {
            let state = self.mempool.get(&msg.tx.hash()).unwrap();
            state.clone()
        };
        self.send_response(origin, state.tx.hash(), state.status.clone());
    }

    /// If k responses are not received within a time bound, the node picks an
    /// additional sample from the remaining nodes uniformly at random and queries
    /// them until it collects all responses.     
    /// TODO: timeout + error handling!
    fn handle_query_response(&mut self, msg: &QueryResponse) -> Option<(Hash, Status)> {
        {
            let mut state = self.mempool.get_mut(&msg.hash).unwrap();
            // If the state is considered final we dont handle this response anymore.
            if state.is_final {
                return None;
            }
            state.responses.push(msg.status.clone());

            let n = state
                .responses
                .iter()
                .filter(|&status| status == &msg.status)
                .count();

            if n >= (TRESHOLD * SAMPLES as f32) as usize {
                // Increment the confidence of the received status.
                let cnt = state.incr_status(&msg.status);
                // Get the confidence of our current status.
                let our_status_cnt = state.status_count(&state.status);

                // If the confidence of the received status is higher then ours we
                // flip to that status.
                if cnt > our_status_cnt {
                    state.status = msg.status.clone();
                    state.last_status = state.status.clone();
                }

                if msg.status != state.last_status {
                    state.last_status = msg.status.clone();
                    state.cnt = 0;
                } else {
                    state.cnt += 1;
                    // We only accept the color (move to the next epoch) if the
                    // counter is higher the the conviction treshold.
                    if state.cnt > (CONVICTION_TRESHOLD * SAMPLES as f32) as u32 {
                        state.advance();
                        if state.epoch == MAX_EPOCHS {
                            state.is_final = true;
                            return Some((state.tx.hash(), state.status.clone()));
                        }
                    }
                }
            }
        }

        let state = self.mempool.get(&msg.hash).unwrap();
        self.send_query(state.tx.clone(), state.status.clone());
        None
    }

    fn handle_transaction(&mut self, tx: &Transaction) {
        // Verify transaction ourself.
        let status = self.verify_transaction(tx);

        // Add the tx to our mempool.
        self.mempool
            .insert(tx.hash(), TxState::new(tx.clone(), status.clone()));
        self.send_query(tx.clone(), status.clone());
    }

    fn send_query(&self, tx: Transaction, status: Status) {
        let msg = Message::Query(QueryMessage {
            tx: tx,
            status: status,
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
