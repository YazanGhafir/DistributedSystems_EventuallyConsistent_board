# coding=utf-8
# ------------------------------------------------------------------------------------------------------
# TDA596 - Lab 1
# server/server.py
# Input: Node_ID total_number_of_ID
# Student: John Doe
# ------------------------------------------------------------------------------------------------------
import traceback
import sys
import time
import json
import argparse
from random import randint
from threading import Thread

from bottle import Bottle, run, request, template
import requests


# ------------------------------------------------------------------------------------------------------
# Event InnerClass
# ------------------------------------------------------------------------------------------------------

# Event class to instantiate and append to the log of event lists,
# each event has eventType A, M or D, the timestamp,
# the senderNodeID and the key value pair massage
class Event:
    def __init__(self, eventType, timestamp, senderNodeID, msg_id, msg_entry):
        self.eventType = eventType
        self.timestamp = timestamp
        self.senderNodeID = senderNodeID
        self.msg_id = msg_id
        self.msg_entry = msg_entry

    def get_msg(self):
        return str(self.msg_id) + '&' + str(self.msg_entry)


# ------------------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------------------


try:
    app = Bottle()

    # The local timestamp of events
    timestamp = 0

    # The List of all events happening in the replica or received from other replicas
    # The elements of this list are objects of Event class that contains
    # eventType A, M or D, the timestamp, the senderNodeID and the key value pair massage
    log = []

    # The board is chosen to be a dictionary
    board = {}


    # ------------------------------------------------------------------------------------------------------
    # BOARD FUNCTIONS
    # You will probably need to modify them
    # ------------------------------------------------------------------------------------------------------

    # Adding element to the dictionary, entry_sequence is the element id (the key) and element is the entry (the value)
    def add_new_element_to_store(entry_sequence, element, is_propagated_call=False):
        global board, node_id
        success = False
        try:
            board[entry_sequence] = element
            success = True
        except Exception as e:
            print(e)
        return success


    # modifying element in the dictionary, entry_sequence is the element id and modified_element is the new entry
    def modify_element_in_store(entry_sequence, modified_element, is_propagated_call=False):
        global board, node_id
        success = False
        try:
            board[entry_sequence] = modified_element
            success = True
        except Exception as e:
            print(e)
        return success


    # deleting element in the dictionary, entry_sequence is the key of the element to be deleted
    def delete_element_from_store(entry_sequence, is_propagated_call=False):
        global board, node_id
        success = False
        try:
            del board[entry_sequence]
            success = True
        except Exception as e:
            print(e)
        return success


    # concurrent connect_to_vessel and propagate
    def concurrent_propagate(path):
        thread = Thread(target=propagate_to_vessels, args=(path,))
        thread.daemon = True
        thread.start()


    def concurrent_connect_to_vessel(vessel_ip, path, payload, req):
        thread = Thread(target=contact_vessel, args=(vessel_ip, path, payload, req))
        thread.daemon = True
        thread.start()

    def log_to_entry ():
        global log
        log_str = ""
        for ev in log:
            log_str = log_str + "<" + str(ev.eventType) + "," + str(ev.timestamp) + "," + str(ev.senderNodeID) + ","+ str(ev.msg_entry) + ">,"
            #log_str = log_str + "<" + str(ev.eventType) + "," + str(ev.timestamp) + "," + str(ev.senderNodeID) + "," + str(ev.msg_id) + "," + str(ev.msg_entry) + "> , "
        return str(log_str)

    # ------------------------------------------------------------------------------------------------------
    # CONSISTENCY FUNCTIONALITY
    # ------------------------------------------------------------------------------------------------------

    # create and append an event to the last of the log list +++
    def locally_create_log_event(eventType, timestamp, senderNodeID, msg):
        global log
        msg_to_list = str(msg).split('&')
        new_id = int(msg_to_list[0])
        new_entry = msg_to_list[1]
        log.append(Event(eventType, timestamp, senderNodeID, new_id, new_entry))
        #print(log[timestamp-1].eventType, log[timestamp-1].timestamp, log[timestamp-1].senderNodeID, log[timestamp-1].msg_id, log[timestamp-1].msg_entry)


    # create and insert an event in the log list in a given index, and return the next index in the list +++
    def create_log_event_at_index(eventType, timestamp, senderNodeID, msg, index):
        global log
        msg_to_list = str(msg).split('&')
        new_id = int(msg_to_list[0])
        new_entry = msg_to_list[1]
        log.insert(index, Event(eventType, timestamp, senderNodeID, new_id, new_entry))
        #print(log[timestamp-1].eventType, log[timestamp-1].timestamp, log[timestamp-1].senderNodeID, log[timestamp-1].msg_id, log[timestamp-1].msg_entry)
        return index + 1


    # locally apply add, modify or delete operations  +++
    def locally_apply_AddModifyDelete(eventType, msg_id, msg_entry):
        if eventType == 'A':
            add_new_element_to_store(msg_id, log_to_entry())
        elif eventType == 'M':
            modify_element_in_store(msg_id, log_to_entry())
        elif eventType == 'D':
            delete_element_from_store(msg_id)


    # rollback functionality +++
    def locally_rollBack_AddModifyDelete(event, index):
        if event.eventType == 'A':
            delete_element_from_store(event.msg_id)
        elif event.eventType == 'M':
            flag = rollBack_modify(event.msg_id, index)
            print("The Modify event rollback return value is : ", flag)
        elif event.eventType == 'D':
            flag = rollBack_delete(event.msg_id, index)
            print("The Modify event rollback return value is : ", flag)

    # roll back function for modify event +++
    def rollBack_modify(id, index):
        global log
        for i in range(index - 1, -1, -1):
            if log[i].msg_id == id:
                if log[i].eventType == 'M' or log[i].eventType == 'A':
                    locally_apply_AddModifyDelete('M', log[i].msg_id, log[i].msg_entry)
                    return True
                else:
                    print("The Log is inconsistent") # come back to it
                    return False
        return False

    # roll back function for delete event +++
    def rollBack_delete(id, index):
        global log
        for i in range(index - 1, -1, -1):
            if log[i].msg_id == id:
                if log[i].eventType == 'M' or log[i].eventType == 'A':
                    locally_apply_AddModifyDelete('A', log[i].msg_id, log[i].msg_entry)
                    return True
                else:
                    print("The Log is inconsistent") # come back to it 2 deletes corner case
                    return False
        return False

    '''
    # roll back function for events with the same time stamp as the coming event +++
    def rollBack_events_same_timestamp(coming_event, first_equal_event_index):
        global log
        i = first_equal_event_index
        while i-1 >= 0 and log[i].timestamp == coming_event.timestamp:
            if coming_event.senderNodeID > log[i].senderNodeID:
                locally_rollBack_AddModifyDelete(log[i], i)
                i = i - 1
            elif coming_event.senderNodeID <= log[i].senderNodeID:
                locally_apply_AddModifyDelete(coming_event, coming_event.msg_id, coming_event.msg_entry)
                create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg(), i + 1)
                i = i + 1
                first_equal_event_index = first_equal_event_index + 1
                break
        redo_from_to(i + 1, first_equal_event_index)
        return first_equal_event_index
    '''

    # roll back function for events with the lower or equal time stamp as the coming event +++
    def consist_the_board(coming_event, index):
        global log
        for i in range(index, -1, -1):
            if coming_event.timestamp < log[i].timestamp:
                locally_rollBack_AddModifyDelete(log[i],i)
            elif coming_event.timestamp == log[i].timestamp:
                #from_ = rollBack_events_same_timestamp(coming_event,i)
                if coming_event.senderNodeID <= log[i].senderNodeID:
                    locally_apply_AddModifyDelete(coming_event.eventType, coming_event.msg_id, coming_event.msg_entry)
                    create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg(), i + 1)
                    redo_from_to(i + 1, index+1) # from = i+2
                    return True
                else :
                    locally_rollBack_AddModifyDelete(log[i], i)
                    locally_apply_AddModifyDelete(coming_event.eventType, coming_event.msg_id, coming_event.msg_entry)
                    create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID,coming_event.get_msg(), i)
                    redo_from_to(i + 1, index+1)
                    return True
            elif coming_event.timestamp > log[i].timestamp:
                locally_apply_AddModifyDelete(coming_event.eventType, coming_event.msg_id, coming_event.msg_entry)
                if i == index :
                    locally_create_log_event(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg())
                    break
                else :
                    create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg(), i + 1)
                redo_from_to(i + 1, index+1) # from = i+2
                return True
        return False

    # redo function applies the events from index to index (problem!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!)
    def redo_from_to(from_, to_):
        global log
        if from_ < len(log) and to_ < len(log):
            for j in range (from_, to_+2):
                locally_apply_AddModifyDelete(log[j].eventType,log[j].msg_id, log[j].msg_entry)
        else:
            print("from = ", from_)
            print("to +2= ", to_ + 2)
            print("len(log) = ", len(log))
            print("Index out of bound! Check for that!")


    def tie_breaker(event1, event2):
        if event1.senderNodeID > event2.senderNodeID: return 1
        else: return 2

    '''
    def consist_the_board(coming_event, index):
        for i in range(index, -1, -1):
            if log[i].timestamp > coming_event.timestamp:
                locally_rollBack_AddModifyDelete(log[i], i)
                if i == 0:
                    locally_apply_AddModifyDelete(coming_event.eventType, coming_event.msg_id, coming_event.msg_entry)
                    create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg(), i)
                    for k in range(i + 1, index + 1): locally_apply_AddModifyDelete(log[k].eventType, log[k].msg_id, log[k].msg_entry)
                    return True
            elif log[i].timestamp == coming_event.timestamp:
                j = i
                while log[j].timestamp == coming_event.timestamp and coming_event.senderNodeID < log[j].senderNodeID:
                    j = j - 1
                locally_apply_AddModifyDelete(coming_event.eventType,coming_event.msg_id, coming_event.msg_entry)
                create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg(), j)
                for k in range(j + 1, index + 1): locally_apply_AddModifyDelete(log[k].eventType, log[k].msg_id, log[k].msg_entry)
                return True
            elif log[i].timestamp < coming_event.timestamp:
                locally_apply_AddModifyDelete(coming_event.eventType,coming_event.msg_id, coming_event.msg_entry)
                create_log_event_at_index(coming_event.eventType, coming_event.timestamp, coming_event.senderNodeID, coming_event.get_msg(), i+1)
                if i + 2 < index + 1:
                    for k in range(i + 2, index + 1): locally_apply_AddModifyDelete(log[k].eventType, log[k].msg_id, log[k].msg_entry) #check index + 2??
                return True
        return False
        '''

    # ------------------------------------------------------------------------------------------------------
    # DISTRIBUTED COMMUNICATIONS FUNCTIONS
    # ------------------------------------------------------------------------------------------------------
    def contact_vessel(vessel_ip, path, payload=None, req='POST'):
        # Try to contact another server (vessel) through a POST or GET, once
        success = False
        try:
            if 'POST' in req:
                res = requests.post('http://{}{}'.format(vessel_ip, path), data=payload)
            elif 'GET' in req:
                res = requests.get('http://{}{}'.format(vessel_ip, path))
            else:
                print('Non implemented feature!')
            # result is in res.text or res.json()
            # print(res.text)
            if res.status_code == 200:
                success = True
        except Exception as e:
            print("")
        return success


    def propagate_to_vessels(path, payload=None, req='POST'):
        global vessel_list, node_id

        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) != node_id:  # don't propagate to yourself
                success = contact_vessel(vessel_ip, path, payload, req)
                if not success:
                    print("")  # print("\n\nCould not contact vessel {}\n\n".format(vessel_id))


    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------
    @app.route('/')
    def index():
        global board, node_id
        return template('server/index.tpl', board_title='Vessel {}'.format(node_id),
                        board_dict=sorted(board.iteritems()), members_name_string='Hassan ghalayini & Yazan Ghafir')


    @app.get('/board')
    def get_board():
        global board, node_id
        print(board)
        #print("--------------------THE LOG------------------")
        #for i in (len(log) - 1, -1, -1):
        #    print("Type = " + log[i].eventType + "timestampe = " + log[i].timestamp + "sende node_id = " + log[i].senderNodeID + "message id = " + log[i].msg_id + "the message = " + log[i].msg_entry)
        #print("--------------------THE LOG------------------")
        return template('server/boardcontents_template.tpl', board_title='Vessel {}'.format(node_id),
                        board_dict=sorted(board.iteritems()))


    # ------------------------------------------------------------------------------------------------------
    # ----------------- ADD ------------------ +++
    # being called whenever a the submit button is clicked,
    # thus an entry is being submitted to the board
    # recieves the post request, reads the entry from the html, 
    # add it to the board and propagate it to the other vessels.
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id, timestamp

        try:
            # reading the entry from the html
            new_entry = request.forms.get('entry')

            keysl = board.keys()
            if len(keysl) == 0:  # checking if the posted entry is the first entry then
                new_id = 0
            else:
                new_id = keysl[-1] + 1  # set its id to 0 other wise set its id to the last entry id +1

            msg = str(new_id) + '&' + str(new_entry)  # creating a message of the id and the entry to be propagated to all other nodes


            timestamp = timestamp + 1

            locally_create_log_event('A', timestamp, node_id, msg)  # Appand the add event to the evenlist
            #entry_to_print = new_entry + "<" + str(timestamp) + "," + str(node_id) + ">"
            entry_to_print = log_to_entry()
            add_new_element_to_store(new_id, entry_to_print)  # adding the posted entry to the board
            concurrent_propagate('/propagate/A/' + str(timestamp) + '/' + str(node_id) + '/' + str(msg))





        except Exception as e:
            print(e)
        return True


    # being called when the button Modify or X is clicked +++
    # ----------------- Modify or Delete ------------------
    # thus an entry is being modified or deleted from the board.
    # It recieves the post request, reads the entry and the delete indicator from the html,
    # modify and propagate if 0 and delete and propagate if 1
    @app.post('/board/<element_id:int>/')
    def client_action_received(element_id):
        global node_id, timestamp
        try:
            # extract the modify/delete indicator which value is 0 or 1 from the html
            modify_delete_indicator = request.forms.get('delete')
            # extract the new entry from the Html
            new_entry = request.forms.get('entry')
            # creating a message of the id and the entry to be propagated to all other nodes
            msg = str(element_id) + '&' + str(new_entry)

            # if the indicator is 0 then modify the vessel's board entry having the element_id to the
            # new entry and propagate to the other vessels with sending the
            # modify_delete_indicator as 0 in the url as the action so the other
            # vessels will understand that it is a modify. the message with the new
            # entry after modifying is aslo sended with its id.
            if str(modify_delete_indicator) == '0':
                modify_element_in_store(element_id, new_entry)
                timestamp = timestamp + 1
                concurrent_propagate('/propagate/M/'+ str(timestamp) + '/' + str(node_id) + '/'+ str(msg))
                locally_create_log_event('M', timestamp, node_id, msg)  # Appand the modify event to the evenlist


            # if the indicator is 1 then delete the entry with its element_id (key-value pair)
            # from the board and propagate to the other vessels with sending the
            # modify_delete_indicator as 1 in the url as the action so the other
            # vessels will understand that it is a delete. the message contaning the element_id and the
            # entry is also sended. Here it would be sufficient to sent just the id but both ways works.
            elif str(modify_delete_indicator) == '1':
                delete_element_from_store(element_id)
                timestamp = timestamp + 1
                concurrent_propagate('/propagate/D/'+ str(timestamp) + '/' + str(node_id) + '/'+ str(msg))
                locally_create_log_event('D', timestamp, node_id, msg)  # Appand the delete event to the evenlist



        except Exception as e:
            print(e)
        return True

        # the main propagation method, being called when a vessel


    # propagate a change to other vessels
    # is activated on each vessel and it checks if the
    # propagated process is a modify, delete or add
    # then the vessel will do the same which is done to to the
    # propagator vessel, meaning, use modif/delete/add methods.
    @app.post('/propagate/<action>/<msg_timestamp>/<sende_node_id>/<msg>')
    def propagation_received(action, msg_timestamp, sende_node_id, msg):
        global timestamp, log
        try:
            # extracting and spliting the sended message to id and entry using the delimiter '&'
            msg_to_list = str(msg).split('&')
            new_id = int(msg_to_list[0])
            new_entry = msg_to_list[1]
            msg_timestamp_int = int(msg_timestamp)
            sende_node_id_int = int(sende_node_id)
            last_log_index = len(log) - 1

            # apply modify/delete/add method on the board, based on the action
            if str(action) == 'M':
                if log[last_log_index].timestamp  < msg_timestamp_int:
                    locally_create_log_event('M', msg_timestamp_int, sende_node_id_int, msg)  # Appand the modify event to the evenlist
                    modify_element_in_store(new_id, new_entry)
                    timestamp = msg_timestamp_int
                elif log[last_log_index].timestamp >= msg_timestamp_int:
                    consist_the_board(Event('M', msg_timestamp_int, sende_node_id_int, new_id, new_entry), len(log) - 1)
                    timestamp = timestamp + 1

            elif str(action) == 'D':
                if log[last_log_index].timestamp  < msg_timestamp_int:
                    locally_create_log_event('D', msg_timestamp_int, sende_node_id_int, msg)  # Appand the delete event to the evenlist
                    delete_element_from_store(new_id)
                    timestamp = msg_timestamp_int
                elif log[last_log_index].timestamp >= msg_timestamp_int:
                    consist_the_board(Event('D', msg_timestamp_int, sende_node_id_int, new_id, new_entry), len(log) - 1)
                    timestamp = timestamp + 1

            elif str(action) == 'A':
                if len(log) == 0 or log[last_log_index].timestamp < msg_timestamp_int:
                    #entry_to_print = new_entry + "<"+str(msg_timestamp)+","+str(sende_node_id)+">"
                    locally_create_log_event('A', msg_timestamp_int, sende_node_id_int, msg)  # Appand the add event to the evenlist
                    entry_to_print = log_to_entry()
                    add_new_element_to_store(new_id, entry_to_print)
                    timestamp = msg_timestamp_int
                elif log[last_log_index].timestamp >= msg_timestamp_int:
                    print("local timestamp is = ", timestamp)
                    print("msg_timestamp_int is = ", msg_timestamp_int)
                    print(" -------------------------------------------------------------------------------------------- ")
                    consist_the_board(Event('A', msg_timestamp_int, sende_node_id_int, new_id, new_entry), len(log) - 1)
                    timestamp = timestamp + 1


        except Exception as e:
            print(e)
        return True


    # ------------------------------------------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------------------------------------------
    def main():
        global vessel_list, node_id, app

        port = 80
        parser = argparse.ArgumentParser(description='Your own implementation of the distributed blackboard')
        parser.add_argument('--id', nargs='?', dest='nid', default=1, type=int, help='This server ID')
        parser.add_argument('--vessels', nargs='?', dest='nbv', default=1, type=int,
                            help='The total number of vessels present in the system')
        args = parser.parse_args()
        node_id = args.nid
        vessel_list = dict()
        # We need to write the other vessels IP, based on the knowledge of their number
        for i in range(1, args.nbv):
            vessel_list[str(i)] = '10.1.0.{}'.format(str(i))

        try:
            run(app, host=vessel_list[str(node_id)], port=port)
        except Exception as e:
            print(e)


    # ------------------------------------------------------------------------------------------------------

    if __name__ == '__main__':
        main()
except Exception as e:
    traceback.print_exc()
    while True:
        time.sleep(60.)
