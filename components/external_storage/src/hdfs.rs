// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path, process::Stdio};

use std::{
    io::{self, BufReader, Read, Seek},
};

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::io::AllowStdIo;
use futures_util::TryStreamExt;
use tokio::{io as async_io, process::Command};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;
use tikv_util::stream::error_stream;
use tikv_util::time::Limiter;

use crate::{ExternalData, ExternalStorage, RestoreConfig, UnpinReader};

/// Convert `hdfs:///path` to `/path`
fn try_convert_to_path(url: &Url) -> &str {
    if url.host().is_none() {
        url.path()
    } else {
        url.as_str()
    }
}

#[derive(Clone, Debug, Default)]
pub struct HdfsConfig {
    pub hadoop_home: String,
    pub hadoop_local_tmp_path: String,
    pub linux_user: String,
}

/// A storage to upload file to HDFS
pub struct HdfsStorage {
    remote: Url,
    config: HdfsConfig,
}

fn check_and_create_dir(dir: &str) -> std::io::Result<()> {
    // Create hadoop tmp path
    let max_retry_num = 10;
    let mut retry_index = 0;
    let retry_interval_ms = 100;

    info!("Before create tmp dir");
    while retry_index < max_retry_num {
        let tmp_path = Path::new(&dir);
        info!("Before create tmp dir"; "tmp path str" => &dir);
        if !tmp_path.exists() || (tmp_path.exists() && !tmp_path.is_dir()) {
            info!("tmp path is not exist");
            if tmp_path.exists() && !tmp_path.is_dir() {
                // Exit but not a dir, delete if first
                std::fs::remove_file(tmp_path)?;
            }

            let create_ret = std::fs::create_dir_all(tmp_path);
            match create_ret {
                Ok(_) => {
                    info!("Create tmp path success"; "tmp path" => &dir);
                    // Chmod to 777
                    chmod_for_dir(&dir)?;
                    break;
                }

                Err(e) => {
                    error!("Create tmp path failed"; "tmp path" => &dir);
                    // retry
                    retry_index += 1;
                    if retry_index == max_retry_num {
                        // Retry to max, just return error
                        error!("Create tmp path failed after max retry time"; "max_retry_num" => max_retry_num);
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("[{}] create failed", &dir),
                        ));
                    }

                    info!("Retry next"; "retry index" => retry_index, "retry_inverval_ms" => retry_interval_ms);
                    std::thread::sleep(Duration::from_millis(retry_interval_ms));
                }
            }
        } else {
            break;
        }
    }
    Ok(())
}

fn chmod_for_dir(dir: &str) -> std::io::Result<()> {
    let mut cmd_with_args = vec![];
    cmd_with_args.extend(["chmod", "-R", "777", dir]);

    // Startup the jvm for hdfs client
    let chmod_cmd = std::process::Command::new(cmd_with_args[0])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .args(&cmd_with_args[1..])
        .spawn()?;

    let output = chmod_cmd.wait_with_output()?;

    // Check hadoop client jvm return message
    if output.status.success() {
        info!("chmod for dir success"; "dir" => dir);
        Ok(())
    } else {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!(
                "chmod returned non-zero status";
                "code" => output.status.code(),
                "stdout" => stdout.as_ref(),
                "stderr" => stderr.as_ref(),
            );
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("chmod returned non-zero status: {:?}", output.status.code()),
        ))
    }
}

impl HdfsStorage {
    pub fn new(remote: &str, config: HdfsConfig) -> io::Result<HdfsStorage> {
        let mut remote = Url::parse(remote).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if !remote.path().ends_with('/') {
            let mut new_path = remote.path().to_owned();
            new_path.push('/');
            remote.set_path(&new_path);
        }

        let check_ret = check_and_create_dir(&config.hadoop_local_tmp_path);
        match check_ret {
            Ok(_) => Ok(HdfsStorage { remote, config }),
            Err(e) => Err(e)
        }
    }

    fn get_hadoop_home(&self) -> Option<String> {
        if self.config.hadoop_home.is_empty() {
            std::env::var("HADOOP_HOME").ok()
        } else {
            Some(self.config.hadoop_home.clone())
        }
    }

    fn get_linux_user(&self) -> Option<String> {
        if self.config.linux_user.is_empty() {
            std::env::var("HADOOP_LINUX_USER").ok()
        } else {
            Some(self.config.linux_user.clone())
        }
    }

    /// Returns `$HDFS_CMD` if exists, otherwise return `$HADOOP_HOME/bin/hdfs`
    fn get_hdfs_bin(&self) -> Option<String> {
        self.get_hadoop_home()
            .map(|hadoop| format!("{}/bin/hdfs", hadoop))
    }

    fn run_hdfs_cmd(&self, name: &str, is_download: bool) -> std::io::Result<()> {
        // Generate absolute file path
        let mut local_file_path_str = String::from(self.config.hadoop_local_tmp_path.clone());
        local_file_path_str += "/";
        local_file_path_str += name;

        // Check parameters
        if name.contains(path::MAIN_SEPARATOR) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in storage", name),
            ));
        }

        let cmd_path = self.get_hdfs_bin().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "Cannot found hdfs command, please specify HADOOP_HOME",
            )
        })?;

        // Generate hdfs path
        let remote_url = self.remote.clone().join(name).unwrap();
        let path = try_convert_to_path(&remote_url);

        // Generate hdfs command
        let remote_url = self.remote.clone().join(name).unwrap();
        let path = try_convert_to_path(&remote_url);

        let mut cmd_with_args = vec![];
        let user = self.get_linux_user();

        if let Some(user) = &user {
            cmd_with_args.extend(["sudo", "-u", user]);
        }

        if is_download {
            cmd_with_args.extend([&cmd_path, "dfs", "-get", path, &local_file_path_str]);
        } else {
            cmd_with_args.extend([&cmd_path, "dfs", "-put", &local_file_path_str, path]);
        }

        info!("calling hdfs"; "cmd" => ?cmd_with_args);

        // Startup the jvm for hdfs client
        let hdfs_cmd = std::process::Command::new(cmd_with_args[0])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .args(&cmd_with_args[1..])
            .spawn()?;

        let output = hdfs_cmd.wait_with_output()?;

        // Check hadoop client jvm return message
        if output.status.success() {
            debug!("save file to hdfs"; "path" => ?path);
            Ok(())
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                "hdfs returned non-zero status";
                "code" => output.status.code(),
                "stdout" => stdout.as_ref(),
                "stderr" => stderr.as_ref(),
            );
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("hdfs returned non-zero status: {:?}", output.status.code()),
            ))
        }
    }

    fn download(&self, name: &str) -> std::io::Result<()> {
        self.run_hdfs_cmd(name, true)
    }

    fn upload(&self, name: &str) -> std::io::Result<()> {
        self.run_hdfs_cmd(name, false)
    }

    fn generate_local_file(&self, name: &str) -> String {
        let mut local_file_path_str = String::from(self.config.hadoop_local_tmp_path.clone());
        local_file_path_str += "/";
        local_file_path_str += name;
        return local_file_path_str;
    }
}

const STORAGE_NAME: &str = "hdfs";

#[async_trait]
impl ExternalStorage for HdfsStorage {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<Url> {
        Ok(self.remote.clone())
    }

    async fn write(&self, name: &str, reader: UnpinReader, _content_length: u64) -> io::Result<()> {
        // Check parameters
        if name.contains(path::MAIN_SEPARATOR) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in storage", name),
            ));
        }
        let cmd_path = self.get_hdfs_bin().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "Cannot found hdfs command, please specify HADOOP_HOME",
            )
        })?;

        // Generate absolute file path
        let local_file_path_str = self.generate_local_file(name);

        // Write to local file
        info!("Start to write data to local file "; "local file name" => &local_file_path_str);
        let f = File::create(&local_file_path_str).await.unwrap();
        let mut buffer = BufWriter::new(f);
        let copy_ret = async_io::copy(&mut reader.0.compat(), &mut buffer).await;
        match copy_ret {
            Ok(_) => info!("Write local file success"; "local file name" => &local_file_path_str),
            Err(e) => {
                // First remove local file
                error!("Write local file failed, remove it now"; "local file name" => &local_file_path_str);
                tokio::fs::remove_file(&local_file_path_str).await?;
                return Err(e);
            }
        }

        // Flush
        let flush_ret = buffer.flush().await;
        match flush_ret {
            Ok(()) => info!("Close and flush local file success"; "local file name" => &local_file_path_str),
            // Flush failed
            Err(e) => {
                // First remove local file
                error!("Close and flush local file success"; "local file name" => &local_file_path_str);
                tokio::fs::remove_file(&local_file_path_str).await?;
                return Err(e);
            }
        }

        // Generate hdfs command
        let remote_url = self.remote.clone().join(name).unwrap();
        let path = try_convert_to_path(&remote_url);

        let mut cmd_with_args = vec![];
        let user = self.get_linux_user();

        if let Some(user) = &user {
            cmd_with_args.extend(["sudo", "-u", user]);
        }
        cmd_with_args.extend([&cmd_path, "dfs", "-put", &local_file_path_str, path]);
        info!("calling hdfs"; "cmd" => ?cmd_with_args);

        // Startup the jvm for hdfs client
        let hdfs_cmd_ret = Command::new(cmd_with_args[0])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .args(&cmd_with_args[1..])
            .spawn();

        // Check startup result
        match hdfs_cmd_ret {
            Ok(_) => info!("Startup JVM for hdfs client success"),
            Err(e) => {
                error!("Startup JVM for hdfs client failed");
                // Remove local file
                tokio::fs::remove_file(&local_file_path_str).await?;
                return Err(e);
            }
        }

        let hdfs_cmd = hdfs_cmd_ret.unwrap();
        let output_ret = hdfs_cmd.wait_with_output().await;
        match output_ret {
            Ok(_) => {
                info!("push local file to hdfs success"; "local file" => &local_file_path_str, "hdfs file" => path);
            }
            Err(e) => {
                info!("push local file to hdfs failed"; "local file" => &local_file_path_str, "hdfs file" => path);
                // Remove local file
                tokio::fs::remove_file(&local_file_path_str).await?;
                return Err(e);
            }
        }

        // Remove local file
        tokio::fs::remove_file(&local_file_path_str).await?;

        // Check hadoop client jvm return message
        let output = output_ret.unwrap();
        if output.status.success() {
            debug!("save file to hdfs"; "path" => ?path);
            Ok(())
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                "hdfs returned non-zero status";
                "code" => output.status.code(),
                "stdout" => stdout.as_ref(),
                "stderr" => stderr.as_ref(),
            );
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("hdfs returned non-zero status: {:?}", output.status.code()),
            ))
        }
    }


    fn read(&self, name: &str) -> ExternalData<'_> {
        // Check file exist or not
        // Generate absolute file path
        let local_file_path_str = self.generate_local_file(name);

        let local_file_path = std::path::Path::new(&local_file_path_str);
        if !local_file_path.exists() {
            // Download if local file does not exist
            let download_ret = self.download(name);
            match download_ret {
                Ok(()) => info!("Download file from hdfs success"; "local file" => &local_file_path_str),
                Err(e) => {
                    error!("Download file from hdfs failed"; "local file" => &local_file_path_str);
                    return Box::new(error_stream(e).into_async_read());
                }
            }
        }

        info!("read file from local storage"; "local file " => &local_file_path_str);
        // We used std i/o here for removing the requirement of tokio reactor when
        // restoring.
        // FIXME: when restore side get ready, use tokio::fs::File for returning.
        match std::fs::File::open(&local_file_path_str) {
            Ok(file) => Box::new(AllowStdIo::new(file)) as _,
            Err(e) => Box::new(error_stream(e).into_async_read()) as _,
        }
    }

    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
        // Check file exist or not
        // Generate absolute file path
        let local_file_path_str = self.generate_local_file(name);

        let local_file_path = std::path::Path::new(&local_file_path_str);
        if !local_file_path.exists() {
            // Download if local file does not exist
            let download_ret = self.download(name);
            match download_ret {
                Ok(()) => info!("Download file from hdfs success"; "local file" => &local_file_path_str),
                Err(e) => {
                    error!("Download file from hdfs failed"; "local file" => &local_file_path_str);
                    return Box::new(error_stream(e).into_async_read());
                }
            }
        }

        info!("read part of file from local storage";
            "local file " => &local_file_path_str, "off" => %off, "len" => %len);

        let mut file = match std::fs::File::open(&local_file_path_str) {
            Ok(file) => file,
            Err(e) => return Box::new(error_stream(e).into_async_read()) as _,
        };
        match file.seek(std::io::SeekFrom::Start(off)) {
            Ok(_) => (),
            Err(e) => return Box::new(error_stream(e).into_async_read()) as _,
        };
        let reader = BufReader::new(file);
        let take = reader.take(len);
        Box::new(AllowStdIo::new(take)) as _
    }

    async fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> io::Result<()> {

        // Generate tmp local file name
        let local_file_path_str = self.generate_local_file(storage_name);

        let RestoreConfig {
            range,
            compression_type,
            expected_sha256,
            file_crypter,
        } = restore_config;

        // Download from hdfs to local storage

        let reader = {
            let inner = if let Some((off, len)) = range {
                self.read_part(storage_name, off, len)
            } else {
                self.read(storage_name)
            };

            super::compression_reader_dispatcher(compression_type, inner)?
        };
        let output = file_system::File::create(restore_name)?;
        // the minimum speed of reading data, in bytes/second.
        // if reading speed is slower than this rate, we will stop with
        // a "TimedOut" error.
        // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
        let min_read_speed: usize = 8192;
        let input = super::encrypt_wrap_reader(file_crypter, reader)?;

        let result = super::read_external_storage_into_file(
            input,
            output,
            speed_limiter,
            expected_length,
            expected_sha256,
            min_read_speed,
        ).await;

        // Remove data file from local storage
        std::fs::remove_file(local_file_path_str)?;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hdfs_bin() {
        let backend = HdfsStorage::new("hdfs://", HdfsConfig::default()).unwrap();
        std::env::remove_var("HADOOP_HOME");
        assert!(backend.get_hdfs_bin().is_none());

        std::env::set_var("HADOOP_HOME", "/opt/hadoop");
        assert_eq!(
            backend.get_hdfs_bin().as_deref(),
            Some("/opt/hadoop/bin/hdfs")
        );

        let backend = HdfsStorage::new(
            "hdfs://",
            HdfsConfig {
                hadoop_home: "/etc/hdfs".to_string(),
                ..Default::default()
            },
        )
            .unwrap();
        assert_eq!(
            backend.get_hdfs_bin().as_deref(),
            Some("/etc/hdfs/bin/hdfs")
        );
    }

    #[test]
    fn test_try_convert_to_path() {
        let url = Url::parse("hdfs:///some/path").unwrap();
        assert_eq!(try_convert_to_path(&url), "/some/path");
        let url = Url::parse("hdfs://1.1.1.1/some/path").unwrap();
        assert_eq!(try_convert_to_path(&url), "hdfs://1.1.1.1/some/path");
    }
}
