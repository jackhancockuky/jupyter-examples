from tabulate import tabulate
import pandas as pd
import logging

import os
import logging

from tabulate import tabulate
from fabrictestbed.util.constants import Constants
from concurrent.futures import ThreadPoolExecutor

from fabrictestbed.slice_editor import (
    ExperimentTopology,
    Capacities
)


class Plugins():        
    @staticmethod
    def load():
        from fabrictestbed_extensions.fablib.slice import Slice
        from fabrictestbed_extensions.fablib.node import Node
        from fabrictestbed_extensions.fablib.network_service import NetworkService
        from fabrictestbed_extensions.fablib.resources import Resources
        from fabrictestbed_extensions.fablib.fablib import FablibManager
        from fabrictestbed_extensions.fablib.resources import Resources
        s = Slice
        setattr( s, 'list_nodes', Plugins.list_nodes )
        setattr( s, 'wait_jupyter', Plugins.wait_jupyter )
        setattr(s, 'show', Plugins.slice_show )
        setattr(s, 'list_interfaces', Plugins.list_interfaces )

        net = NetworkService
        setattr(net, 'show', Plugins.network_show )

        n = Node
        setattr(n, 'show', Plugins.node_show )


        resources = Resources
        setattr(resources, 'list', Plugins.list_resources )

        fm = FablibManager
        setattr(fm, 'print_table', Plugins.print_table )
        setattr(fm, 'create_table', Plugins.create_table )
        

        setattr(fm, 'show_config', Plugins.show_config)
        setattr(fm, '__init__', Plugins.fm__init__)

        

    def wait_jupyter(self, timeout=600, interval=10):
        from IPython.display import clear_output
        import time
        import random
        global fablib

        start = time.time()

        count = 0
        while not self.isStable():
            if time.time() > start + timeout:
                raise Exception(f"Timeout {timeout} sec exceeded in Jupyter wait")

            time.sleep(interval)
            self.update()
            # node_list = self.list_nodes()

            #pre-get the strings for quicker screen update
            # slice_string=str(self)

            table = [["Slice Name", self.sm_slice.slice_name],
                 ["Slice ID", self.sm_slice.slice_id],
                 ["Slice State", self.sm_slice.slice_state],
                 ["Lease End (UTC)", self.sm_slice.lease_end]
                ]

            time_string = f"{time.time() - start:.0f} sec"

            # Clear screen
            clear_output(wait=True)

            #Print statuses
            self.get_fablib_manager().print_table(table, title='Slice Reservation Status', properties={'text-align': 'left', 'border': '1px black solid !important'}, hide_header=True, title_font_size='1.25em')

            print(f"\nRetry: {count}, Time: {time_string}")

            self.list_nodes()

            count += 1

        print(f"\nTime to stable {time.time() - start:.0f} seconds")

        #print("Running wait_ssh ... ", end="")
        #self.wait_ssh()
        #print(f"Time to ssh {time.time() - start:.0f} seconds")

        print("Running post_boot_config ... ", end="")
        self.post_boot_config()
        print(f"Time to post boot config {time.time() - start:.0f} seconds")

        if len(self.get_interfaces()) > 0:
            print(f"\n{self.list_interfaces()}")
            print(f"\nTime to print interfaces {time.time() - start:.0f} seconds")


    def list_nodes(self, verbose=False):
        if not verbose:
            table = []
            for node in self.get_nodes():

                table.append( [     node.get_reservation_id(),
                                    node.get_name(),
                                    node.get_site(),
                                    node.get_host(),
                                    node.get_cores(),
                                    node.get_ram(),
                                    node.get_disk(),
                                    node.get_image(),
                                    node.get_management_ip(),
                                    node.get_reservation_state(),
                                    node.get_error_message(),
                                    ] )
                
            headers=["ID", "Name",  "Site",  "Host", "Cores", "RAM", "Disk", "Image",
                                            "Management IP", "State", "Error"]
            printable_table = self.get_fablib_manager().create_table(table, title=f'List of Nodes in {self.get_name()}', properties={'text-align': 'left'}, headers=headers, index='Name')

                                    
            if(self.get_fablib_manager().output_type.lower() == 'text'):
                print(printable_table)
                return

            elif(self.get_fablib_manager().output_type.lower() == 'html'):
                def highlight(x):
                    if x.State == 'Ticketed':
                        return ['background-color: yellow']*(len(headers)-1)
                    elif x.State == 'None':
                        return ['opacity: 50%']*(len(headers)-1)
                    else:
                        return ['background-color: ']*(len(headers)-1)

                def green_active(val):
                    if val == 'Active':
                        color = 'green'
                    else:
                        color = 'black'
                    return 'color: %s' % color

                printable_table = printable_table.apply(highlight, axis=1).applymap(green_active, subset=pd.IndexSlice[:, ['State']]).set_properties(**{'text-align': 'left'})

                display(printable_table)
                return

        else:
            col = [ "Name",
                    "Site", 
                    "ID", 
                    "Cores", 
                    "RAM", 
                    "Disk", 
                    "Image", 
                    "SSH Command",
                    "Image Type",
                    "Host", 
                    "Management IP",
                    "Reservation State",
                    "Error Message"
                    ]
                
            df = pd.DataFrame()

            for node in self.get_nodes():
                table = [   [node.get_name()],
                            [node.get_site()],
                            [node.get_reservation_id()],
                            [node.get_cores()],
                            [node.get_ram()],
                            [node.get_disk()],
                            [node.get_image()],
                            [node.get_ssh_command()],
                            [node.get_image_type()],
                            [node.get_host()],
                            [node.get_management_ip()],
                            [node.get_reservation_state()],
                            [node.get_error_message()]
                            ]

                table = [*zip(*table)]
                df2 = pd.DataFrame(table, columns=col)

                df = pd.concat([df, df2], ignore_index = True)

            df.set_index('Name', inplace=True, drop=True)
            df.columns.name = df.index.name
            df.index.name = None

            if(self.get_fablib_manager().output_type.lower() == 'text'):
                print(df.to_string())

            elif(self.get_fablib_manager().output_type.lower() == 'html'):
                tt = pd.DataFrame([['','','','','','','The command to use to connect to the node via SSH. Copy, paste, and run this command in a terminal.','','','The IP address used to connect to the node from outside the FABRIC network. This is not the IP address used to connect between nodes in the FABRIC network.','','']],
                    index=df.index, columns=df.columns)
                
                style = df.style.set_caption(f"{self.get_name()} Node Information").set_properties(**{'text-align': 'left', 'white-space': 'nowrap', 'border': '1px black solid !important'}).set_sticky(axis='index').set_table_styles([{
                        'selector': 'caption',
                        'props': 'caption-side: top; font-size:1.25em;'
                    }], overwrite=False).set_tooltips(tt, props='visibility: hidden; position: absolute; z-index: 1; border: 1px solid #000066;'
                            'background-color: white; color: #000066; font-size: 1.2em;'
                            'transform: translate(-200%, -50px); padding: 0.6em; border-radius: 0.5em; white-space: nowrap;')

                display(style)
            return


    def print_table(self, table, headers=None, title='', properties={}, hide_header=False, title_font_size='1.25em', index=None):
        if(self.output_type.lower() == 'text'):
            print(f"\n{self.create_table(table, headers=headers, title=title, properties=properties, hide_header=hide_header, title_font_size=title_font_size,index=index)}")

        elif(self.output_type.lower() == 'html'):
            display(self.create_table(table, headers=headers, title=title, properties=properties, hide_header=hide_header, title_font_size=title_font_size,index=index))


    def create_table(self, table, headers=None, title='', properties={}, hide_header=False, title_font_size='1.25em', index=None):
        if(self.output_type.lower() == 'text'):
            if headers is not None:
                slice_string = tabulate(table, headers=headers)
            else:
                slice_string = tabulate(table)
            return slice_string

        elif(self.output_type.lower() == 'html'):
            if headers is not None:
                df = pd.DataFrame(table, columns=headers)
            else:
                df = pd.DataFrame(table)

            if index is not None:
                df.set_index(index, inplace=True, drop=True)
                df.columns.name = df.index.name
                df.index.name = None

            if hide_header:
                style = df.style.set_caption(title).set_properties(**properties).hide(axis='index').hide(axis='columns').set_table_styles([{
                    'selector': 'caption',
                    'props': f'caption-side: top; font-size:{title_font_size};'
                }], overwrite=False)
            else:
                style = df.style.set_caption(title).set_properties(**properties).set_table_styles([{
                        'selector': 'caption',
                        'props': f'caption-side: top; font-size:{title_font_size};'
                    }], overwrite=False)
            
            slice_string = style
            return slice_string


    def slice_show(self):
        table = [["Slice Name", self.sm_slice.slice_name],
                 ["Slice ID", self.sm_slice.slice_id],
                 ["Slice State", self.sm_slice.slice_state],
                 ["Lease End (UTC)", self.sm_slice.lease_end]
                ]

        self.get_fablib_manager().print_table(table, title='Slice Information', properties={'text-align': 'left', 'border': '1px black solid !important'}, hide_header=True)


    def network_show(self):
        table = [ ["ID", self.get_reservation_id()],
            ["Name", self.get_name()],
            ["Layer", self.get_layer()],
            ["Type", self.get_type()],
            ["Site", self.get_site()],
            ["Gateway", self.get_gateway()],
            ["L3 Subnet", self.get_subnet()],
            ["Reservation State", self.get_reservation_state()],
            ["Error Message", self.get_error_message()],
            ]
                
        self.get_fablib_manager().print_table(table, title='Network Information', properties={'text-align': 'left', 'border': '1px black solid !important'}, hide_header=True)


    def list_interfaces(self):
        from concurrent.futures import ThreadPoolExecutor

        executor = ThreadPoolExecutor(10)

        net_name_threads = {}
        node_name_threads = {}
        physical_os_interface_name_threads = {}
        os_interface_threads = {}
        for iface in self.get_interfaces():
            if iface.get_network():
                logging.info(f"Starting get network name thread for iface {iface.get_name()} ")
                net_name_threads[iface.get_name()] = executor.submit(iface.get_network().get_name)

            if iface.get_node():
                logging.info(f"Starting get node name thread for iface {iface.get_name()} ")
                node_name_threads[iface.get_name()] = executor.submit(iface.get_node().get_name)

            logging.info(f"Starting get physical_os_interface_name_threads for iface {iface.get_name()} ")
            physical_os_interface_name_threads[iface.get_name()] = executor.submit(iface.get_physical_os_interface_name)

            logging.info(f"Starting get get_os_interface_threads for iface {iface.get_name()} ")
            os_interface_threads[iface.get_name()] = executor.submit(iface.get_os_interface)

        table = []
        for iface in self.get_interfaces():

            if iface.get_network():
                #network_name = iface.get_network().get_name()
                logging.info(f"Getting results from get network name thread for iface {iface.get_name()} ")
                network_name = net_name_threads[iface.get_name()].result()
            else:
                network_name = None

            if iface.get_node():
                #node_name = iface.get_node().get_name()
                logging.info(f"Getting results from get node name thread for iface {iface.get_name()} ")
                node_name = node_name_threads[iface.get_name()].result()

            else:
                node_name = None

            table.append( [     iface.get_name(),
                                node_name,
                                network_name,
                                iface.get_bandwidth(),
                                iface.get_vlan(),
                                iface.get_mac(),
                                physical_os_interface_name_threads[iface.get_name()].result(),
                                os_interface_threads[iface.get_name()].result(),
                                ] )

        headers=["Name", "Node", "Network", "Bandwidth", "VLAN", "MAC", "Physical OS Interface", "OS Interface"]
        self.get_fablib_manager().print_table(table, title='Slice Interfaces Information', properties={'text-align': 'left'}, headers=headers, index='Name')


    def node_show(self):
        table = [ ["ID", self.get_reservation_id()],
            ["Name", self.get_name()],
            ["Cores", self.get_cores()],
            ["RAM", self.get_ram()],
            ["Disk", self.get_disk()],
            ["Image", self.get_image()],
            ["Image Type", self.get_image_type()],
            ["Host", self.get_host()],
            ["Site", self.get_site()],
            ["Management IP", self.get_management_ip()],
            ["Reservation State", self.get_reservation_state()],
            ["Error Message", self.get_error_message()],
            ["SSH Command ", self.get_ssh_command()],
            ]

        self.get_fablib_manager().print_table(table, title='Node Information', properties={'text-align': 'left', 'border': '1px black solid !important'}, hide_header=True)
                

    def show_config(self):
        table = []
        for var, val in self.get_config().items():
            table.append([str(var), str(val)])
            
        self.get_fablib_manager().print_table(table, title='User Configuration for FABlib Manager', properties={'text-align': 'left', 'border': '1px black solid !important'}, hide_header=True)


    def list_resources(self):
        table = []
        for site_name, site in self.topology.sites.items():
            #logging.debug(f"site -- {site}")
            table.append( [     site.name,
                                self.get_cpu_capacity(site_name),
                                f"{self.get_core_available(site_name)}/{self.get_core_capacity(site_name)}",
                                f"{self.get_ram_available(site_name)}/{self.get_ram_capacity(site_name)}",
                                f"{self.get_disk_available(site_name)}/{self.get_disk_capacity(site_name)}",
                                #self.get_host_capacity(site_name),
                                #self.get_location_postal(site_name),
                                #self.get_location_lat_long(site_name),
                                f"{self.get_component_available(site_name,'SharedNIC-ConnectX-6')}/{self.get_component_capacity(site_name,'SharedNIC-ConnectX-6')}",
                                f"{self.get_component_available(site_name,'SmartNIC-ConnectX-6')}/{self.get_component_capacity(site_name,'SmartNIC-ConnectX-6')}",
                                f"{self.get_component_available(site_name,'SmartNIC-ConnectX-5')}/{self.get_component_capacity(site_name,'SmartNIC-ConnectX-5')}",
                                f"{self.get_component_available(site_name,'NVME-P4510')}/{self.get_component_capacity(site_name,'NVME-P4510')}",
                                f"{self.get_component_available(site_name,'GPU-Tesla T4')}/{self.get_component_capacity(site_name,'GPU-Tesla T4')}",
                                f"{self.get_component_available(site_name,'GPU-RTX6000')}/{self.get_component_capacity(site_name,'GPU-RTX6000')}",
                                ] )

        headers=["Name",
                "CPUs",
                "Cores",
                f"RAM ({Capacities.UNITS['ram']})",
                f"Disk ({Capacities.UNITS['disk']})",
                #"Workers"
                #"Physical Address",
                #"Location Coordinates"
                "Basic (100 Gbps NIC)",
                "ConnectX-6 (100 Gbps x2 NIC)",
                "ConnectX-5 (25 Gbps x2 NIC)",
                "P4510 (NVMe 1TB)",
                "Tesla T4 (GPU)",
                "RTX6000 (GPU)",
                ]
                
        self.get_fablib_manager().print_table(table, title=f'Current Available Resources', properties={'text-align': 'left'}, headers=headers, index='Name')


    def fm__init__(self,
                 fabric_rc=None,
                 credmgr_host=None,
                 orchestrator_host=None,
                 fabric_token=None,
                 project_id=None,
                 bastion_username=None,
                 bastion_key_filename=None,
                 log_level=None,
                 log_file=None,
                 data_dir=None,
                 output_type='text'):
        """
        Constructor. Builds FablibManager.  Tries to get configuration from:

         - constructor parameters (high priority)
         - fabric_rc file (middle priority)
         - environment variables (low priority)
         - defaults (if needed and possible)

        """
        # super().__init__() #FIXME: Remove from original code?

        self.output_type = output_type

        #initialized thread pool for ssh connections
        self.ssh_thread_pool_executor = ThreadPoolExecutor(10)

        # init attributes
        self.bastion_passphrase = None
        self.log_file = self.default_log_file
        self.log_level = self.default_log_level
        self.set_log_level(self.log_level)

        #self.set_log_file(log_file)
        self.data_dir = data_dir

        # Setup slice key dict
        # self.slice_keys = {}
        self.default_slice_key = {}

        # Set config values from env vars
        if Constants.FABRIC_CREDMGR_HOST in os.environ:
            self.credmgr_host = os.environ[Constants.FABRIC_CREDMGR_HOST]

        if Constants.FABRIC_ORCHESTRATOR_HOST in os.environ:
            self.orchestrator_host = os.environ[Constants.FABRIC_ORCHESTRATOR_HOST]

        if Constants.FABRIC_TOKEN_LOCATION in os.environ:
            self.fabric_token = os.environ[Constants.FABRIC_TOKEN_LOCATION]

        if Constants.FABRIC_PROJECT_ID in os.environ:
            self.project_id = os.environ[Constants.FABRIC_PROJECT_ID]

        # Basstion host setup
        if self.FABRIC_BASTION_USERNAME in os.environ:
            self.bastion_username = os.environ[self.FABRIC_BASTION_USERNAME]
        if self.FABRIC_BASTION_KEY_LOCATION in os.environ:
            self.bastion_key_filename = os.environ[self.FABRIC_BASTION_KEY_LOCATION]
        if self.FABRIC_BASTION_HOST in os.environ:
            self.bastion_public_addr = os.environ[self.FABRIC_BASTION_HOST]
        # if self.FABRIC_BASTION_HOST_PRIVATE_IPV4 in os.environ:
        #    self.bastion_private_ipv4_addr = os.environ[self.FABRIC_BASTION_HOST_PRIVATE_IPV4]
        # if self.FABRIC_BASTION_HOST_PRIVATE_IPV6 in os.environ:
        #    self.bastion_private_ipv6_addr = os.environ[self.FABRIC_BASTION_HOST_PRIVATE_IPV6]

        # Slice Keys
        if self.FABRIC_SLICE_PUBLIC_KEY_FILE in os.environ:
            self.default_slice_key['slice_public_key_file'] = os.environ[self.FABRIC_SLICE_PUBLIC_KEY_FILE]
            with open(os.environ[self.FABRIC_SLICE_PUBLIC_KEY_FILE], "r") as fd:
                self.default_slice_key['slice_public_key'] = fd.read().strip()
        if self.FABRIC_SLICE_PRIVATE_KEY_FILE in os.environ:
            # self.slice_private_key_file=os.environ['FABRIC_SLICE_PRIVATE_KEY_FILE']
            self.default_slice_key['slice_private_key_file'] = os.environ[self.FABRIC_SLICE_PRIVATE_KEY_FILE]
        if "FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE" in os.environ:
            # self.slice_private_key_passphrase = os.environ['FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE']
            self.default_slice_key['slice_private_key_passphrase'] = os.environ[
                self.FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE]

        # Set config values from fabric_rc file
        if fabric_rc == None:
            fabric_rc = self.default_fabric_rc

        fabric_rc_dict = self.read_fabric_rc(fabric_rc)

        if Constants.FABRIC_CREDMGR_HOST in fabric_rc_dict:
            self.credmgr_host = fabric_rc_dict[Constants.FABRIC_CREDMGR_HOST]

        if Constants.FABRIC_ORCHESTRATOR_HOST in fabric_rc_dict:
            self.orchestrator_host = fabric_rc_dict[Constants.FABRIC_ORCHESTRATOR_HOST]

        if 'FABRIC_TOKEN_LOCATION' in fabric_rc_dict:
            self.fabric_token = fabric_rc_dict['FABRIC_TOKEN_LOCATION']
            os.environ[Constants.FABRIC_TOKEN_LOCATION] = self.fabric_token

        if 'FABRIC_PROJECT_ID' in fabric_rc_dict:
            self.project_id = fabric_rc_dict['FABRIC_PROJECT_ID']
            os.environ['FABRIC_PROJECT_ID'] = self.project_id

        # Basstion host setup
        if self.FABRIC_BASTION_HOST in fabric_rc_dict:
            self.bastion_public_addr = fabric_rc_dict[self.FABRIC_BASTION_HOST]
        if self.FABRIC_BASTION_USERNAME in fabric_rc_dict:
            self.bastion_username = fabric_rc_dict[self.FABRIC_BASTION_USERNAME]
        if self.FABRIC_BASTION_KEY_LOCATION in fabric_rc_dict:
            self.bastion_key_filename = fabric_rc_dict[self.FABRIC_BASTION_KEY_LOCATION]
        if self.FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE in fabric_rc_dict:
            self.bastion_key_filename = fabric_rc_dict[self.FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE]

        # Slice keys
        if self.FABRIC_SLICE_PRIVATE_KEY_FILE in fabric_rc_dict:
            self.default_slice_key['slice_private_key_file'] = fabric_rc_dict[self.FABRIC_SLICE_PRIVATE_KEY_FILE]
        if self.FABRIC_SLICE_PUBLIC_KEY_FILE in fabric_rc_dict:
            self.default_slice_key['slice_public_key_file'] = fabric_rc_dict[self.FABRIC_SLICE_PUBLIC_KEY_FILE]
            with open(fabric_rc_dict[self.FABRIC_SLICE_PUBLIC_KEY_FILE], "r") as fd:
                self.default_slice_key['slice_public_key'] = fd.read().strip()
        if self.FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE in fabric_rc_dict:
            self.default_slice_key['slice_private_key_passphrase'] = fabric_rc_dict[
                self.FABRIC_SLICE_PRIVATE_KEY_PASSPHRASE]

        if self.FABRIC_LOG_FILE in fabric_rc_dict:
            self.set_log_file(fabric_rc_dict[self.FABRIC_LOG_FILE])
        if self.FABRIC_LOG_LEVEL in fabric_rc_dict:
            self.set_log_level(fabric_rc_dict[self.FABRIC_LOG_LEVEL])

        # Set config values from constructor arguments
        if credmgr_host != None:
            self.credmgr_host = credmgr_host
        if orchestrator_host != None:
            self.orchestrator_host = orchestrator_host
        if fabric_token != None:
            self.fabric_token = fabric_token
        if project_id != None:
            self.project_id = project_id
        if bastion_username != None:
            self.bastion_username = bastion_username
        if bastion_key_filename != None:
            self.bastion_key_filename = bastion_key_filename
        if log_level != None:
            self.set_log_level(log_level)
        if log_file != None:
            self.set_log_file(log_file)
        if data_dir != None:
            self.data_dir = data_dir

        # self.bastion_private_ipv4_addr = '0.0.0.0'
        # self.bastion_private_ipv6_addr = '0:0:0:0:0:0'

        # Create slice manager
        self.slice_manager = None
        self.resources = None
        self.build_slice_manager()