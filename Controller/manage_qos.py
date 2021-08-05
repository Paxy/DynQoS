import os
import sys
import time
import json
import mysql.connector as mysql


class QoSControll:
    def __init__(self, switch_ip_and_management_port, qos_uuid, queue_num):
        self.command_base = "ovs-vsctl --db=tcp:" + switch_ip_and_management_port
        self.qos_uuid = qos_uuid
        self.queue_num = queue_num
        self.queue_uuid = None

    def add_queue(self, max_rate):
        new_queue = self.command_base + " set qos " + self.qos_uuid + \
            " queues:" + self.queue_num + "=@queue" + self.queue_num + " -- --id=@queue" + self.queue_num + \
            " create queue other-config:max-rate=" + max_rate
        self.queue_uuid = os.popen(new_queue).read().split('\n')[0]

    def set_queue(self, max_rate):
        edited_queue = self.command_base + " set queue " + self.queue_uuid + " other-config=max-rate=" + max_rate
        os.system(edited_queue)

    def del_queue(self):
        rem_queue = self.command_base + " remove qos " + self.qos_uuid + " queue " + self.queue_num
        del_queue = self.command_base + " destroy queue " + self.queue_uuid
        os.system(rem_queue)
        os.system(del_queue)
        self.queue_uuid = None


class QueueControll:
    def __init__(self, car_switch_manage, car_switch_control, car_qos_uuid, bs_switch_manage, bs_switch_control, bs_qos_uuid, queue_num):
        self.queue_num = queue_num
        self.car_queue = QoSControll(car_switch_manage, car_qos_uuid, queue_num)
        self.bs_queue = QoSControll(bs_switch_manage, bs_qos_uuid, queue_num)
        self.db_connection = None
        self.db_cursor = None
        self.latest_query_result = None
        self.car_switch_control = car_switch_control
        self.bs_switch_control = bs_switch_control
        
    def add_flows(self):
        add_car_flow = "ovs-ofctl add-flow tcp:" + self.car_switch_control + " dl_vlan=" + self.queue_num + ",actions=set_queue:" + self.queue_num + ",normal"
        add_bs_flow = "ovs-ofctl add-flow tcp:" + self.bs_switch_control + " dl_vlan=" + self.queue_num + ",actions=set_queue:" + self.queue_num + ",normal"
        os.system(add_car_flow)
        os.system(add_bs_flow)
        
    def del_flows(self):
        del_car_flow = "ovs-ofctl del-flows tcp:" + self.car_switch_control + " dl_vlan=" + self.queue_num
        del_bs_flow = "ovs-ofctl del-flows tcp:" + self.bs_switch_control + " dl_vlan=" + self.queue_num
        os.system(del_car_flow)
        os.system(del_bs_flow)

    def add_queues(self, car_max_rate, bs_max_rate):
        self.car_queue.add_queue(car_max_rate)
        self.bs_queue.add_queue(bs_max_rate)

    def set_queues(self, car_max_rate, bs_max_rate):
        self.car_queue.set_queue(car_max_rate)
        self.bs_queue.set_queue(bs_max_rate)

    def del_queues(self):
        self.car_queue.del_queue()
        self.bs_queue.del_queue()

    def fetch_queue_from_db(self, db_ip, db_user, db_pass, db_name):
        self.db_connection = mysql.connect(host=db_ip, user=db_user, passwd=db_pass, database=db_name)
        self.db_cursor = self.db_connection.cursor()
        self.db_cursor.execute("SELECT download, upload FROM services WHERE vlan = " + self.queue_num)
        queue_result = self.db_cursor.fetchone()
        self.db_cursor.close()
        self.db_connection.close()
        return queue_result

    def update_queues_on_switches(self, db_ip, db_user, db_pass, db_name):
        query_result = self.fetch_queue_from_db(db_ip, db_user, db_pass, db_name)

        if query_result is None:
            if self.car_queue.queue_uuid is None:
                pass
            else:
                self.del_flows()
                self.del_queues()
        else:
            if self.car_queue.queue_uuid is None:
                self.add_queues(str(query_result[1]), str(query_result[0]))
                self.add_flows()
                self.latest_query_result = query_result
            else:
                if query_result != self.latest_query_result:
                    self.set_queues(str(query_result[1]), str(query_result[0]))
                    self.latest_query_result = query_result
                else:
                    pass


if __name__ == '__main__':
    queue_vlan = sys.argv[1]
    refresh_period = sys.argv[2]

    with open("config.json", "r") as config_file:
        config_params = json.load(config_file)

    queues_on_switch = QueueControll(config_params["car_switch_manage"], config_params["car_switch_control"], config_params["car_qos_uuid"],
                                     config_params["base_switch_manage"], config_params["base_switch_control"], config_params["base_qos_uuid"],
                                     queue_vlan)

    try:
        while True:
            queues_on_switch.update_queues_on_switches(config_params["db_ip"], config_params["db_user"],
                                                       config_params["db_pass"], config_params["db_name"])

            time.sleep(int(refresh_period))
    except KeyboardInterrupt:
        if queues_on_switch.db_cursor is not None:
            queues_on_switch.db_cursor.close()

        if queues_on_switch.db_connection is not None:
            queues_on_switch.db_connection.close()

        if queues_on_switch.car_queue.queue_uuid is not None:
            queues_on_switch.del_flows()
            queues_on_switch.del_queues()
            
        print("Program terminated by user")

