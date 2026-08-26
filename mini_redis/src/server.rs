use crate::store::Db;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn read_resp_array(
    reader: &mut BufReader<TcpStream>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut line = String::new();

    let bytes_read = reader.read_line(&mut line).await?;
    if bytes_read == 0 {
        return Err("Connection closed".into());
    }

    if !line.starts_with('*') {
        return Err("Expected RESP array".into());
    }

    let count: usize = line[1..].trim().parse()?;
    let mut args = Vec::new();

    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await?;

        if !line.starts_with('$') {
            return Err("Expected RESP bulk string".into());
        }

        let len: usize = line[1..].trim().parse()?;

        let mut buf = vec![0; len];
        reader.read_exact(&mut buf).await?;
        args.push(String::from_utf8(buf)?);

        let mut trailing = [0; 2];
        reader.read_exact(&mut trailing).await?;
    }

    Ok(args)
}

// Note the 'pub' keyword here so main.rs can use it
pub async fn handle_client(stream: TcpStream, db: Db) {
    let mut reader = BufReader::new(stream);

    loop {
        let parts = match read_resp_array(&mut reader).await {
            Ok(parts) => parts,
            Err(_) => break,
        };

        if parts.is_empty() {
            continue;
        }

        let command = parts[0].to_uppercase();
        let mut response = String::new();

        match command.as_str() {
            "GET" => {
                if parts.len() < 2 {
                    response = "-ERR wrong number of arguments for 'get' command\r\n".to_string();
                } else if let Some(val) = db.get(&parts[1]) {
                    response = format!("${}\r\n{}\r\n", val.len(), val);
                } else {
                    response = "$-1\r\n".to_string();
                }
            }
            "SET" => {
                if parts.len() < 3 {
                    response = "-ERR wrong number of arguments for 'set' command\r\n".to_string();
                } else {
                    db.set(parts[1].clone(), parts[2].clone(), None);
                    response = "+OK\r\n".to_string();
                }
            }
            "DEL" => {
                if parts.len() < 2 {
                    response = "-ERR wrong number of arguments for 'del' command\r\n".to_string();
                } else {
                    let deleted = db.del(&parts[1]);
                    response = if deleted {
                        ":1\r\n".to_string()
                    } else {
                        ":0\r\n".to_string()
                    };
                }
            }
            // NEW: EXPIRE command parser
            "EXPIRE" => {
                if parts.len() < 3 {
                    response = "-ERR wrong number of arguments for 'expire' command\r\n".to_string();
                } else {
                    match parts[2].parse::<u64>() {
                        Ok(secs) => {
                            let updated = db.expire(&parts[1], secs);
                            response = if updated {
                                ":1\r\n".to_string()
                            } else {
                                ":0\r\n".to_string()
                            };
                        }
                        Err(_) => {
                            response = "-ERR value is not an integer or out of range\r\n".to_string();
                        }
                    }
                }
            }
            "SAVE" => {
                response = "-ERR manual save is disabled (AOF is enabled)\r\n".to_string();
            }
            _ => {
                response = format!("-ERR unknown command '{}'\r\n", command);
            }
        }

        if reader.get_mut().write_all(response.as_bytes()).await.is_err() {
            break;
        }
    }
}