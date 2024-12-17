from apachelogs import LogParser
# I have been using apachelogs for a while now its better than using Regex. Since using apachelogs lib here. 
from collections import Counter
import logging

logging.basicConfig(level=logging.INFO)

def pre_process_log_line(line):
    # On line 8899 I have observed that line is incorrect. I have also captured the error. After looking into it I have observed that '"' was missing. 
    # Handling that over here. 
    line = line.rstrip('\n')
    if line.count('"') % 2 != 0:
        logging.info(f'line has odd number of quotes')
        line = line + '"'
        return line
    else: 
        logging.error(f'something different its not a quote issue')
        return None

def main(): 
    try:

        file_name = input("Enter the log file name: ").strip()
        # file_name = "./interview-apache-logs/examples/example2.log" # for testing
        parser = LogParser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"") # apachelogs directly supports apache log formates - https://httpd.apache.org/docs/current/mod/mod_log_config.html

        total_requests = 0
        total_data_bytes = 0
        resource_counter = Counter()
        host_counter = Counter()
        status_counter = Counter()

        line_index = 1 # for my ref

        with open(file_name, 'r') as f:
            for line in f: 
                try:
                    data = parser.parse(line)
                    host = data.remote_host
                    status = data.final_status
                    bytes = data.bytes_sent
                    if not bytes:
                        bytes = 0
                    request_line = data.directives["%r"]
                    request_parts = request_line.split()
                    if len(request_parts) >= 2:
                        resource = request_parts[1]
                    else: 
                        resource = request_line
                except Exception as e:
                    # Printing the error for ref
                    logging.error(f'Error: {e}')
                    logging.error(f'Line_index - {line_index}')
                    logging.info(f'Handling the line error by checking the quotes')
                    pre_process_line = pre_process_log_line(line)
                    # handling double quotes issue here
                    try:
                        data = parser.parse(pre_process_line)
                        host = data.remote_host
                        status = data.final_status
                        bytes = data.bytes_sent
                        if not bytes:
                            bytes = 0
                        request_line = data.directives["%r"] # output of this consist of entire info something like 'GET /favicon.ico HTTP/1.1' but we are only interested in resource. Thats why splitting it. 
                        request_parts = request_line.split()
                        if len(request_parts) >= 2:
                            resource = request_parts[1]
                        else: 
                            resource = request_line
                    except Exception as e:
                        logging.error(f'Failed to parse preprocessed line: {e}')
                        logging.error(f'Line_index - {line_index}')
                        line_index += 1
                        continue
                    
                # Process the parsed data
                total_requests += 1
                total_data_bytes += int(bytes)
                resource_counter[resource] += 1
                host_counter[host] += 1
                status_counter[status] += 1
                line_index += 1


        if total_requests == 0:
            print("No data to process")
            return
        
        # total requests for a file
        logging.info(f'total no of requests -- {total_requests}')

        # total data transferred 
        logging.info(f'total data transferred -- {total_data_bytes} bytes')

        # most requested resource
        top_resource, top_resource_count = resource_counter.most_common(1)[0]
        top_resource_percent = (top_resource_count / total_requests) * 100

        logging.info(f'most requested resource -- {top_resource}  -- total count for the resource -- {top_resource_count} -- Percentage of requests for --  ({top_resource_percent:.2f}%)')

        # host with most requests
        top_host, top_host_count = host_counter.most_common(1)[0]
        top_host_percent = (top_host_count / total_requests) * 100

        logging.info(f'host with most requests -- {top_host} -- {top_host_count} -- ({top_host_percent:.2f}%)')

        status_code_classes = {
            '1xx': 0,
            '2xx': 0,
            '3xx': 0,
            '4xx': 0,
            '5xx': 0
        }

        for code, count in status_counter.items():
            if code >= 100 and code < 200:
                status_code_classes['1xx'] += count
            if code >= 200 and code < 300:
                status_code_classes['2xx'] += count
            elif code >= 300 and code < 400:
                status_code_classes['3xx'] += count
            elif code >= 400 and code < 500:
                status_code_classes['4xx'] += count
            elif code >= 500 and code < 600:
                status_code_classes['5xx'] += count
            
        code_classes_percentage = {
            '1xx': (status_code_classes['1xx'] / total_requests) * 100,
            '2xx': (status_code_classes['2xx'] / total_requests) * 100,
            '3xx': (status_code_classes['3xx'] / total_requests) * 100,
            '4xx': (status_code_classes['4xx'] / total_requests) * 100,
            '5xx': (status_code_classes['5xx'] / total_requests) * 100
        }

        for code_class, percetage in code_classes_percentage.items():
            logging.info(f'{code_class} -- {percetage:.2f}%')

    except Exception as e:
        logging.error(f'Error: {e}')
        logging.error(f'Line_index - {line_index}')

if __name__ == '__main__':
    main()
    