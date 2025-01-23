// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Range;

use futures::executor::block_on;

use coprocessor_plugin_api::*;

mod graph_meta_proto;
//use graph_meta_proto::coprocessor::*;
use protobuf::{Message, Enum, CodedInputStream};
use protobuf::rustproto::exts::lite_runtime_all;
use crate::graph_meta_proto::coprocessor::{CountRequestV2, CountResponseV2, PaginationRequest, PaginationResponse, RequestType};
use crate::graph_meta_proto::coprocessor::RequestType::COUNT;
use crate::graph_meta_proto::coprocessor::RequestType::PAGINATION;

mod graph_meta_cache;

#[derive(Default)]
struct GraphMetaPlugin;

fn encode_u64(value: &u64) -> Vec<u8> {
    let mut ret: Vec<u8> = Vec::with_capacity(8);
    ret.push((value & 0xff) as u8);
    ret.push(((value >> 8) & 0xff) as u8);
    ret.push(((value >> 16) & 0xff) as u8);
    ret.push(((value >> 24) & 0xff) as u8);
    ret.push(((value >> 32) & 0xff) as u8);
    ret.push(((value >> 40) & 0xff) as u8);
    ret.push(((value >> 48) & 0xff) as u8);
    ret.push(((value >> 56) & 0xff) as u8);
    ret
}

fn from_big_endian(value: &Vec<u8>) -> i32 {
    let mut ret = ((value[0] & 0xff) as i32) << 24;
    ret += ((value[1] & 0xff) as i32) << 16;
    ret += ((value[2] & 0xff) as i32) << 8;
    ret += (value[3] & 0xff) as i32;
    return ret;
}

impl CoprocessorPlugin for GraphMetaPlugin {
    fn on_raw_coprocessor_request(
        &self,
        _ranges: Vec<Range<Key>>,
        _request: RawRequest,
        _storage: &dyn RawStorage,
    ) -> PluginResult<RawResponse> {
        let request_type_num = from_big_endian(&_request);
        let mut is = CodedInputStream::from_bytes(&_request);
        is.skip_raw_bytes(4).expect("TODO: panic message");
        let request_type = RequestType::from_i32(request_type_num).unwrap();

        return match request_type {
            COUNT => {
                let count_request = CountRequestV2::parse_from(&mut is).unwrap();
                //let count_request = CountRequestV2::parse_from_bytes(_request.as_ref()).unwrap();
                let scan_batch_limit = count_request.limit as usize;
                let scan_range = _ranges.first().unwrap().clone();

                // Generate response
                let mut count_response = CountResponseV2::new();

                let scan_result_future = _storage.scan_key_only_with_limit(scan_range, scan_batch_limit);
                let scan_result = block_on(scan_result_future);
                if scan_result.is_err() {
                    count_response.error = scan_result.as_ref().err().unwrap().to_string();
                }

                let pairs = scan_result.as_ref().unwrap();
                let last_kv = pairs.last().unwrap().clone().0;

                //panic!("scan over, scan_batch_limit: {}, scan result size: {}", scan_batch_limit, pairs.len());
                //panic!("scan over"; "scan_batch_limit" => scan_batch_limit, "scan result size = " => pairs.len());

                count_response.counter = pairs.len() as i64;
                count_response.lastKey = last_kv;
                let serialize_ret = count_response.write_to_bytes().unwrap();
                Ok(serialize_ret)
            }

            PAGINATION => {
                let pagination_request = PaginationRequest::parse_from(&mut is).unwrap();
                // Generate response
                let mut pagination_response = PaginationResponse::new();
                let offset = pagination_request.offset as usize;
                let limit = pagination_request.limit as usize;
                let scan_batch_limit = 10000;
                let mut scan_offset: usize = 0;

                // Skip
                let mut scan_range = _ranges.first().unwrap().clone();
                loop {
                    if scan_offset >= offset {
                        break;
                    }

                    let actual_batch_limit = if scan_offset + scan_batch_limit < offset {
                        scan_batch_limit
                    } else {
                        offset - scan_offset
                    };

                    let scan_result_future = _storage.scan_key_only_with_limit(scan_range.clone(), actual_batch_limit);
                    let scan_result = block_on(scan_result_future);
                    if scan_result.is_err() {
                        pagination_response.error = scan_result.err().unwrap().to_string();
                        return Ok(pagination_response.write_to_bytes().unwrap());
                    }

                    let data_rows = scan_result.unwrap();
                    let scan_size = data_rows.len();
                    scan_offset += scan_size;
                    if scan_size < actual_batch_limit {
                        // Scan over, break
                        break;
                    }

                    let (last_key, _last_value): &KvPair = data_rows.last().unwrap();
                    scan_range = Range {
                        start: get_next_range(last_key.clone()),
                        end: scan_range.clone().end
                    };
                }

                if scan_offset < offset {
                    pagination_response.error = format!("Total rows in range is {}, but offset request is {}", scan_offset, offset);
                    return Ok(pagination_response.write_to_bytes().unwrap());
                }

                let scan_result_future = _storage.scan_with_limit(scan_range.clone(), limit);
                let scan_result = block_on(scan_result_future);
                if scan_result.is_err() {
                    pagination_response.error = scan_result.as_ref().err().unwrap().to_string();
                } else {
                    let mut iter = scan_result.as_ref().unwrap().iter();
                    loop {
                        let kv = iter.next();
                        if kv.is_none() {
                            break;
                        } else {
                            let mut easygraph_kv = graph_meta_proto::coprocessor::KvPair::new();
                            easygraph_kv.key = kv.unwrap().clone().0;
                            easygraph_kv.value = kv.unwrap().clone().1;
                            pagination_response.kvs.push(easygraph_kv)
                        }
                    }
                }

                let serialize_ret = pagination_response.write_to_bytes().unwrap();
                Ok(serialize_ret)

                /*
                let scan_range = _ranges.first().unwrap().clone();
                let scan_result_future = _storage.scan_key_only_with_limit(scan_range.clone(), offset);
                let scan_result = block_on(scan_result_future);
                if scan_result.is_err() {
                    pagination_response.error = scan_result.as_ref().err().unwrap().to_string();
                }
                let skip_row_num = scan_result.as_ref().unwrap().len();
                if skip_row_num < offset {
                    pagination_response.error = String::from("Total row number is less than offset");
                } else {
                    let pairs = scan_result.as_ref().unwrap();
                    let last_kv = pairs.last().unwrap().clone().0;
                    let scan_range = Range {
                        start: get_next_range(last_kv.clone()),
                        end: scan_range.clone().end
                    };

                    let scan_result_future = _storage.scan_with_limit(scan_range, limit);
                    let scan_result = block_on(scan_result_future);
                    if scan_result.is_err() {
                        pagination_response.error = scan_result.as_ref().err().unwrap().to_string();
                    } else {
                        let mut iter = scan_result.as_ref().unwrap().iter();
                        loop {
                            let kv = iter.next();
                            if kv.is_none() {
                                break;
                            } else {
                                let mut easygraph_kv = graph_meta_proto::coprocessor::KvPair::new();
                                easygraph_kv.key = kv.unwrap().clone().0;
                                easygraph_kv.value = kv.unwrap().clone().1;
                                pagination_response.kvs.push(easygraph_kv)
                            }
                        }
                    }
                }

                let serialize_ret = pagination_response.write_to_bytes().unwrap();
                Ok(serialize_ret)

                 */
            }
        }

        /*
        // Count
        let mut counter: i64 = 0;
        for range in &_ranges {
            let mut scan_range = range.clone();
            loop {
                let scan_result_future = _storage.scan_key_only_with_limit(scan_range, scan_batch_limit);
                let scan_result = block_on(scan_result_future);
                if scan_result.is_err() {
                    return Err(scan_result.err().unwrap());
                }

                let data_rows = scan_result.unwrap();
                let scan_size = data_rows.len() as i64;
                counter += scan_size;
                if scan_size <= 0 {
                    break;
                }

                let (last_key, _last_value): &KvPair = data_rows.last().unwrap();
                scan_range = Range {
                    start: get_next_range(last_key.clone()),
                    end: range.clone().end
                };
            }
        }

        // Generate response
        let mut count_response = CountResponse::new();
        count_response.counter = counter;
        let serialize_ret = count_response.write_to_bytes().unwrap();
        return Ok(serialize_ret);

         */


        /*
        match request_type {
            0 => {
                // Update graph meta data
                let request_data = &_request[1..request_len];
                // Parse
                let graph_meta = graph_meta_proto::meta::GraphMeta::parse_from_bytes(request_data).unwrap();

                // Insert to cache
                let graph_id = graph_meta.id;
                let mut graph_meta_cache = GRAPH_META_CACHE.lock().unwrap();
                graph_meta_cache.cache.insert(graph_id, graph_meta);

                // Just for test
                let result_bytes = graph_meta_cache.cache.get(&graph_id).unwrap().write_to_bytes().unwrap();
                return PluginResult::Ok(result_bytes);
            },
            1 => {
                // Count
                let mut counter:u64 = 0;
                for range in &_ranges {
                    let scan_result_future = _storage.scan(range.clone());
                    let scan_result = block_on(scan_result_future);
                    let data_rows = scan_result.unwrap();
                    println!("scan result {} ", data_rows.len());
                    counter += data_rows.len() as u64;
                }
        
                println!("Total scan result is {}", counter);
                let serialize_ret = encode_u64(&counter);
                return PluginResult::Ok(serialize_ret);
            }, 

            2 => {
                // Get graph meta data
                let graph_id = 0;
                let mut graph_meta_cache = GRAPH_META_CACHE.lock().unwrap();

                // Just for test
                let result_bytes = graph_meta_cache.cache.get(&graph_id).unwrap().write_to_bytes().unwrap();
                return PluginResult::Ok(result_bytes);
            }

            3 => {
                // Parse key/value
                

                for range in &_ranges {
                    let scan_result_future = _storage.scan(range.clone());
                    let scan_result = block_on(scan_result_future);
                    let data_rows = scan_result.unwrap();
                    
                    for data_row in data_rows {
                        let mut is = CodedInputStream::from_bytes(data_row);
                    }

                    println!("scan result {} ", data_rows.len());
                    counter += data_rows.len() as u64;
                }
        
            }

            _ => PluginResult::Err(PluginError::Other(String::from("Not support retquest type new"), Box::new(1))),
        }
        */
    }


}

fn get_next_range(start_key: Key) -> Key {
    let mut new_start_key = start_key.clone();
    new_start_key.push(0);
    return new_start_key;
}

declare_plugin!(GraphMetaPlugin::default());
