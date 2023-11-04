#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc},
    net::{IpAddr, SocketAddr, Ipv4Addr}, error::Error, fmt
};

use tokio::{
    sync::{broadcast,Mutex,RwLock},
    net::{TcpListener},
    io::{AsyncRead, AsyncWrite}
};
use clap::Parser;
use axum::{
    debug_handler,
    Router,
    routing,
    response::{IntoResponse},
    extract,
    http::{StatusCode},
    Json
};
use axum_tungstenite::{
    WebSocket,
    WebSocketUpgrade,
    Message, UrlError
};
use futures::{
    sink::SinkExt,
    stream::{StreamExt, FusedStream}, future::BoxFuture, FutureExt
};
use serde::{Serialize,Deserialize};
use tokio_tungstenite::{WebSocketStream, connect_async};
use tower::filter::AsyncPredicate;
use tracing::{event, Level, instrument};
use async_recursion::async_recursion;

#[derive(Parser,Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    port: Option<u16>,

    #[arg(short, long)]
    listen: Option<String>,

    // Domain to advertise this servers location
    #[arg(short, long)]
    domain: Option<String>,

    #[arg(long,short,action=clap::ArgAction::Count,)]
    verbose: u8,

    #[arg(long,short)]
    quiet: bool
}

fn get_verbosity(v: u8) -> Level {
    match v {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        4..=std::u8::MAX => Level::TRACE
    }
}

#[derive(Debug)]
enum ServerError {
    UrlError,
    ConError
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match self {
            ServerError::UrlError => "Url Error",
            ServerError::ConError => "Connection Error",
            _ => "Server Error" 
        })
    }
}

impl Error for ServerError {}

/*
 * Clients send to cl_tx and fed_tx
 * Clients read from cl_tx
 * Servers Send to cl_tx and
 * Servers read from fed_tx
 */
struct Channel {
    cl_tx: broadcast::Sender<String>,
    fed_tx: broadcast::Sender<String>,
    peers: Mutex<HashSet<String>>,
    name: String,
}

impl Channel {
    fn new(name: &str) -> Self {
        Self {
            cl_tx: broadcast::Sender::new(32),
            fed_tx: broadcast::Sender::new(32),
            peers: Mutex::new(HashSet::new()),
            name: String::from(name)
        }
    }
}

struct AppState {
    channels: Mutex<HashMap<String, Arc<Channel>>>,
    domain: String
}

async fn ch_connection<S: 'static + AsyncRead + AsyncWrite + Unpin + Send>(
    ch: Arc<Channel>,
    sock: WebSocketStream<S>,
    mut rx: broadcast::Receiver<String>,
    is_cl: bool,
    cl_name: String
) -> () {
    if !is_cl {
        event!(Level::INFO, "Adding {} to peers of {}", &cl_name, &ch.name);
        let mut lock = ch.peers.lock().await;
        lock.insert(cl_name.clone());
    }

    event!(Level::INFO, "Opening connection to {}/{}", &cl_name, &ch.name);
    let (mut send, mut recv) = sock.split();

    let lname_copy = cl_name.clone();
    let cname_copy = ch.name.clone();
    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            event!(Level::TRACE, "Sending message to {}/{}", &lname_copy, &cname_copy);
            if send.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Recv messages from clients
    let lname_copy = cl_name.clone();
    let cname_copy = ch.name.clone();
    let cl_tx = ch.cl_tx.clone();
    let fed_tx = ch.fed_tx.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = recv.next().await {
            event!(Level::TRACE, "Recieved message from {}/{}", &lname_copy, &cname_copy);
            let _ = cl_tx.send(text.clone());
            if is_cl {let _ = fed_tx.send(text);}
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
    event!(Level::INFO, "Closing connection to {}/{}", &cl_name, &ch.name);

    if !is_cl {
        event!(Level::INFO, "Removing {} from peers of {}", &cl_name, &ch.name);
        let mut lock = ch.peers.lock().await;
        lock.remove(&cl_name);
    }
}

async fn remove_peer(ch: Arc<Channel>, dom: &str) {
    {
        let mut lock = ch.peers.lock().await;
        lock.remove(dom);
    }
}

#[async_recursion]
async fn fed(
    state: Arc<AppState>,
    ch: Arc<Channel>,
    dom: &str, id: &str
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ch_url: String = format!("{}/ch/{}", dom, id);
    event!(Level::INFO, "Attempting to federate with {}", &ch_url);

    {
        let lock = ch.peers.lock().await;
        if lock.contains(dom) {
            event!(Level::INFO, "Existing federation with {}", &ch_url);
            return(Ok(()));
        }
    }

    let ws_url: String = format!("ws://{}/fed/{}?n={}", dom, id, &state.domain);
    if let Ok((sock, _)) = connect_async(&ws_url).await {
        let mut rx = ch.fed_tx.subscribe();
        let chcopy = ch.clone();
        let domcopy = String::from(dom);
        tokio::spawn(async move {
            ch_connection(chcopy, sock, rx, false, domcopy).await;
        });
    } else {
        event!(Level::WARN, "Unable to federate with {}", &ws_url);
        return Err::<(), Box<dyn Error + Send + Sync>>(Box::new(ServerError::ConError));
    }

    let peers_url: String = format!("http://{}/peers", &ch_url);
    event!(Level::DEBUG, "Getting peers of {}", &peers_url);
    let mut peers = reqwest::get(peers_url)
        .await?
        .json::<Vec<String>>()
        .await?;
    while let Some(p) = peers.pop() {
        event!(Level::DEBUG, "Got peer {}", &p);
        // Really fucking inefficient but async shenanigans
        let state_copy = state.clone();
        let chcopy = ch.clone();
        let idcopy = String::from(id);
        tokio::spawn(async move {
            fed(state_copy, chcopy, &p, &idcopy).await;
        });
    }
    Ok::<(), Box<dyn Error + Sync + Send>>(())
}

#[derive(Deserialize, Debug)]
struct ChPost {
    name: String,
    join: Option<String>
}

#[debug_handler]
async fn ch_post(
    extract::State(state): extract::State<Arc<AppState>>,    
    Json(body): Json<ChPost>
) -> impl IntoResponse {
    event!(Level::DEBUG, "Channel post {:?}", &body);
    let newch;
    {   // Mutex guard scope
        let mut ch_list = state.channels.lock().await;
        if ch_list.contains_key(&body.name) {
            event!(Level::DEBUG, "Channel already exists {}", &body.name);
            return (StatusCode::CONFLICT, "Channel already exists").into_response();
        }
        newch = Arc::new(Channel::new(&body.name));
        ch_list.insert(body.name.clone(), newch.clone());
    }
    event!(Level::INFO, "Created channel {:?}", &body);
    if let Some(dom) = body.join {
        fed(state, newch.clone(), &dom, &body.name).await;
    }
    StatusCode::OK.into_response()
}

#[debug_handler]
async fn ch_get(
    extract::State(state): extract::State<Arc<AppState>>    
) -> impl IntoResponse {
    event!(Level::DEBUG, "Getting channel list");
    let json_r: String;
    {
        let lock = state.channels.lock().await;
        let ch_list: Vec<&str> = lock.iter()
            .map(|(k, v)| k.as_str())
            .collect();
        json_r = serde_json::to_string(&ch_list).unwrap();
    }
    (StatusCode::OK, json_r)
}

#[debug_handler]
async fn ch_peer_hdl(
    extract::Path(id): extract::Path<String>,
    extract::State(state): extract::State<Arc<AppState>>    
) -> impl IntoResponse {
    event!(Level::DEBUG, "Peer request {}", &id);
    let ch: Arc<Channel>;
    {
        let chlist = state.channels.lock().await;
        if let Some(inch) = chlist.get(&id) {
            ch = inch.clone();
        } else {
            event!(Level::DEBUG, "Channel not found in peer request {}", &id);
            return (
                StatusCode::NOT_FOUND,
                "Channel not found"
            ).into_response();
        };
    }
    let json_r: String;
    {
        let peerset = ch.peers.lock().await;
        let peers: Vec<&str> = peerset.iter()
            .map(|s| s.as_str())
            .collect();
        json_r = serde_json::to_string(&peers).unwrap();
    }
    (StatusCode::OK, json_r).into_response()
}

#[debug_handler]
async fn ch_ws_hdl(
    ws: WebSocketUpgrade,
    extract::ConnectInfo(client): extract::ConnectInfo<SocketAddr>,
    extract::Path(id): extract::Path<String>,
    extract::State(state): extract::State<Arc<AppState>>
) -> impl IntoResponse {
    event!(Level::DEBUG, "Recieved channel ws connection {}", id);
    let ch: Arc<Channel>;
    {
        let chlist = state.channels.lock().await;
        if let Some(inch) = chlist.get(&id) {
            ch = inch.clone();
        } else {
            event!(Level::DEBUG, "Channel not found {}", &id);
            return (
                StatusCode::NOT_FOUND,
                "Channel not found"
            ).into_response();
        };
    }
    let mut rx = ch.cl_tx.subscribe();
    let cl_name = client.to_string();
    ws.on_upgrade(move |sock| ch_connection(ch, sock.into_inner(), rx, true, cl_name))
}

#[derive(Deserialize)]
struct FedQ {
    n: String
}

#[debug_handler]
async fn fed_ws_hdl(
    ws: WebSocketUpgrade,
    extract::ConnectInfo(client): extract::ConnectInfo<SocketAddr>,
    extract::Query(query): extract::Query<FedQ>,
    extract::Path(id): extract::Path<String>,
    extract::State(state): extract::State<Arc<AppState>>
) -> impl IntoResponse {
    event!(Level::DEBUG, "Recieved channel ws connection from {} at {} with {}", client.to_string(), &id, &query.n);
    let ch: Arc<Channel>;
    {
        let chlist = state.channels.lock().await;
        if let Some(inch) = chlist.get(&id) {
            ch = inch.clone();
        } else {
            event!(Level::DEBUG, "Channel not found {}", id);
            return (
                StatusCode::NOT_FOUND,
                "Channel not found"
            ).into_response();
        };
    }
    let mut rx = ch.fed_tx.subscribe();
    let cl_name = query.n.clone();
    
    ws.on_upgrade(move |sock| ch_connection(ch, sock.into_inner(), rx, false, cl_name))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    
    if !cli.quiet { 
        tracing_subscriber::fmt()
            .with_max_level(get_verbosity(cli.verbose))
            .init();
    }
    event!(Level::TRACE, "Running with args {:?}", &cli);
    let listen_addr: SocketAddr = SocketAddr::new(
        match &cli.listen {
            Some(x) => x.parse().unwrap(),
            None => IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        },
        cli.port.unwrap_or(22500)
    );

    let app_state = Arc::new(AppState {
        channels: Mutex::new(HashMap::new()),
        domain: cli.domain.unwrap_or(format!("{}:{}", &cli.listen.unwrap_or(String::from("127.0.0.1")), cli.port.unwrap_or(22500)))
    });

    event!(Level::INFO, "Starting server on domain {}",  &app_state.domain);

    let app = Router::new()
        .route("/ch", routing::get(ch_get).post(ch_post))
        .route("/ch/:id", routing::get(ch_ws_hdl))
        .route("/ch/:id/peers", routing::get(ch_peer_hdl))
        .route("/fed/:id", routing::get(fed_ws_hdl))
        .with_state(app_state);

    axum::Server::bind(&listen_addr)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}
