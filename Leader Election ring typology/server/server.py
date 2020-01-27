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
try:
    app = Bottle()

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
            print(res.text)
            if res.status_code == 200:
                success = True
        except Exception as e:
            print(e)
        return success


    def propagate_to_vessels(path, payload=None, req='POST'):
        global vessel_list, node_id

        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) != node_id:  # don't propagate to yourself
                success = contact_vessel(vessel_ip, path, payload, req)
                if not success:
                    print("\n\nCould not contact vessel {}\n\n".format(vessel_id))


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
        return template('server/boardcontents_template.tpl', board_title='Vessel {}'.format(node_id),
                        board_dict=sorted(board.iteritems()))


    # ------------------------------------------------------------------------------------------------------

    # being called whenever a the submitt button is clicked, 
    # thus an entry is being submitted to the board
    # recieves the post request, reads the entry from the html, 
    # add it to the board and propagate it to the other vessels.
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id

        try:
            # reading the entry from the html
            new_entry = request.forms.get('entry')

            # checking if the posted entry is the first entry then
            # set its id to 0 other wise set its id to the last entry id +1
            keysl = board.keys()
            if len(keysl) == 0:
                new_id = 0
            else:
                new_id = keysl[-1] + 1

                # adding the posted entry to the board
            add_new_element_to_store(new_id, new_entry)

            # creating a message of the id and the entry to be propagated to all other nodes
            msg = str(new_id) + '&' + str(new_entry)

            # Assigning the the propagating mission to a thread to avoid blocking
            ppath = "/propagate/add/" + str(msg)
            print("path ", ppath)
            thread = Thread(target=propagate_to_vessels, args=(ppath,))
            thread.daemon = True
            thread.start()

        except Exception as e:
            print(e)
        return True

        # being called when the button Modify or X is clicked


    # thus an entry is being modified or deleted from the board.
    # It recieves the post request, reads the entry and the delete indicator from the html, 
    # modify and propagate if 0 and delete and propagate if 1
    @app.post('/board/<element_id:int>/')
    def client_action_received(element_id):

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
                propagate_to_vessels('/propagate/0/' + msg)

            # if the indicator is 1 then delete the entry with its element_id (key-value pair)
            # from the board and propagate to the other vessels with sending the
            # modify_delete_indicator as 1 in the url as the action so the other
            # vessels will understand that it is a delete. the message contaning the element_id and the
            # entry is also sended. Here it would be sufficient to sent just the id but both ways works.
            elif str(modify_delete_indicator) == '1':
                delete_element_from_store(element_id)
                propagate_to_vessels('/propagate/1/' + msg)


        except Exception as e:
            print(e)
        return True

        # the main propagation method, being called when a vessel


    # propagate a change to other vessels
    # is activated on each vessel and it checks if the 
    # propagated process is a modify, delete or add 
    # then the vessel will do the same which is done to to the 
    # propagator vessel, meaning, use modif/delete/add methods.
    @app.post('/propagate/<action>/<msg>')
    def propagation_received(action, msg):

        try:
            # extracting and spliting the sended message to id and entry using the delimiter '&'
            msg_to_list = str(msg).split('&')
            new_id = int(msg_to_list[0])
            new_entry = msg_to_list[1]

            # apply modify/delete/add method on the board, based on the action
            if str(action) == '0':
                modify_element_in_store(new_id, new_entry)

            elif str(action) == '1':
                delete_element_from_store(new_id)

            else:
                add_new_element_to_store(new_id, new_entry)


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
