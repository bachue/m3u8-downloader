use anyhow::{Error, Result};
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use m3u8_rs::{
    parse_playlist,
    playlist::{MasterPlaylist, MediaPlaylist, Playlist},
};
use once_cell::sync::Lazy;
use reqwest::{Client, Response};
use std::{
    env,
    io::SeekFrom,
    mem::take,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{create_dir_all, write, File, OpenOptions},
    io::AsyncWriteExt,
};
use url::Url;

static HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);

#[tokio::main]
async fn main() -> Result<()> {
    let url =
        Url::parse(&env::args().nth(1).expect("URL must be given")).expect("URL must be valid");
    let m3u8_filename = {
        let mut segment = url
            .path_segments()
            .and_then(|segments| segments.last())
            .expect("At least 1 path segment expected")
            .to_owned();
        if !segment.ends_with(".m3u8") {
            segment.push_str(".m3u8");
        }
        segment
    };
    let mut playlist = choose_media_playlist(vec![url]).await?;

    {
        let ts_dir_name = {
            let mut dir = PathBuf::new();
            dir.push(m3u8_filename.to_owned() + ".ts");
            dir
        };
        create_dir_all(&ts_dir_name).await?;
        download_ts_and_replace(&ts_dir_name, &mut playlist).await?;
    }

    {
        let mut m3u8_content = Vec::new();
        playlist.write_to(&mut m3u8_content).unwrap();
        write(&m3u8_filename, m3u8_content).await?;
    }

    Ok(())
}

fn choose_media_playlist(urls: Vec<Url>) -> BoxFuture<'static, Result<MediaPlaylist>> {
    async move {
        let mut last_error: Option<Error> = None;
        for url in urls.into_iter() {
            println!("Get M3U8: {}", url);
            match HTTP_CLIENT
                .get(url.as_str())
                .send()
                .and_then(|resp| resp.bytes())
                .await
            {
                Ok(bytes) => match parse_playlist(&bytes) {
                    Ok((_, Playlist::MasterPlaylist(playlist))) => {
                        return choose_media_playlist(choose_urls_from_master_playlist(
                            playlist, &url,
                        ))
                        .await
                    }
                    Ok((_, Playlist::MediaPlaylist(mut playlist))) => {
                        normalize_media_playlist(&mut playlist, &url);
                        return Ok(playlist);
                    }
                    Err(err) => {
                        last_error = Some(Error::msg(err.to_string()));
                    }
                },
                Err(err) => {
                    last_error = Some(err.into());
                }
            }
        }
        Err(last_error.unwrap())
    }
    .boxed()
}

fn choose_urls_from_master_playlist(mut playlist: MasterPlaylist, original_url: &Url) -> Vec<Url> {
    let variants = take(&mut playlist.variants);
    let best_bandwidth = variants
        .iter()
        .max_by_key(|variant| variant.bandwidth.parse::<u64>().unwrap())
        .map(|variant| variant.bandwidth.parse::<u64>().unwrap())
        .unwrap();
    variants
        .into_iter()
        .filter(|variant| variant.bandwidth.parse::<u64>().unwrap() >= best_bandwidth)
        .map(|variant| variant.uri)
        .map(|uri| {
            Url::parse(&uri).unwrap_or_else(|_| {
                Url::options()
                    .base_url(Some(&original_url))
                    .parse(&uri)
                    .unwrap()
            })
        })
        .collect()
}

fn normalize_media_playlist(playlist: &mut MediaPlaylist, original_url: &Url) {
    for segment in playlist.segments.iter_mut() {
        segment.uri = Url::parse(&segment.uri)
            .unwrap_or_else(|_| {
                Url::options()
                    .base_url(Some(&original_url))
                    .parse(&segment.uri)
                    .unwrap()
            })
            .to_string();
    }
}

async fn download_ts_and_replace(dir: &Path, playlist: &mut MediaPlaylist) -> Result<()> {
    for (id, segment) in playlist.segments.iter_mut().enumerate() {
        let mut file_path = dir.to_path_buf();
        file_path.push(format!("{}.ts", id));
        let ts_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .await?;
        download_ts_to(&segment.uri, ts_file).await?;
        segment.uri = file_path.into_os_string().into_string().unwrap();
    }
    Ok(())
}

async fn download_ts_to(url: &str, mut file: File) -> Result<()> {
    const RETRIES: u8 = 10;
    let mut start = file.seek(SeekFrom::End(0)).await?;

    loop {
        let mut response: Option<Response> = None;
        for retried in 0u8..RETRIES {
            let mut req = HTTP_CLIENT.get(url);
            if start > 0 {
                req = req.header("Range", format!("{}-", start));
            }
            match req.send().await {
                Ok(resp) => {
                    response = Some(resp);
                    break;
                }
                Err(err) => {
                    eprintln!("HTTP Send Error ({} / {}): {}", retried, RETRIES, err);
                }
            }
        }

        let mut response =
            response.unwrap_or_else(|| panic!("Too many times to be failed to get {}", url));

        let mut retried = 0;
        while retried < RETRIES {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    file.write_all(&chunk).await?;
                    start += chunk.len() as u64;
                    retried = 0;
                }
                Ok(None) => {
                    file.flush().await?;
                    println!("Get TS: {}", url);
                    return Ok(());
                }
                Err(err) => {
                    retried += 1;
                    eprintln!("HTTP Get Body Error ({} / {}): {}", retried, RETRIES, err);
                }
            }
        }
        if retried >= RETRIES {
            panic!("Too many times to be failed to get data from {}", url);
        }
    }
}
