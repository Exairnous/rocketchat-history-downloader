"""
Description:
    Download and store raw JSON channel history for joined standard
    channels and direct messages. Specify start and/or end date bounds
    or use defaults of room creation and yesterday (respectively)

Dependencies:
    pipenv install
        rocketchat_API - Python API wrapper for Rocket.Chat
            https://github.com/jadolg/rocketchat_API
            (pipenv install rocketchat_API)

    Actual Rocket.Chat API
        https://rocket.chat/docs/developer-guides/rest-api/channels/history/

Configuration:
    settings.cfg contains Rocket.Chat login information and file paths

Commands:
    pipenv run python export-history.py settings.cfg
    pipenv run python export-history.py -s 2000-01-01 -e 2018-01-01 -r settings.cfg
    etc

Notes:
    None

Author:
    Ben Willard <willardb@gmail.com> (https://github.com/willardb)
"""

# DONE: Add this style to header <style>p {margin:0px;}</style>
# DONE: Add "Reacted by" text (plus count) to reaction
# DONE: Remove extra colons from reaction emoji name
# DONE: Add margin top to reactions and remove margin bottom from images
# DONE: Add margin top to quotes
# DONE: Add whether and when a message was edited to message title
# DONE: Add support for 'MENTION_CHANNEL' and 'BIG_EMOJI' - test with day 2023-04-22
# DONE: Change to using global class styles rather than inlining
# DONE: Download attachments to file-specific folder e.g. 2023-05-21-test_01_attachments
# DONE: Add channel/dm/group to filename (date-type-name)
# DONE: Fix arguments to naming functions to be safer
# DONE: Get thread messages (requires patch to rocketchat_API, or not if you use call_api_get hack)
# DONE: List number of messages in thread in brackets after Message Replies text like so Message Replies (50)
# DONE: Allow processing of JSON files separately to convert to HTML (all or date range)
# DONE: Make automatic conversion of JSON to HTML optional when downloading the JSON and attachments
# DONE: Decide whether to show thread replies in main channel in HTML (can't always get referenced message)
# DONE: Add overflow-y: auto;max-height: 200px; to reaction class css
# DONE: Fix message_requests date to work with multiple days
# TODO: Consider adding https://pypi.org/project/emoji/ support to allow rendering of emoji shortcodes

import datetime
import pickle
import os
import logging
import pprint
import argparse
import configparser
import re
from time import sleep
from rocketchat_API.rocketchat import RocketChat
import requests
import urllib.parse
import json
import html


#
# Initialize stuff
#
VERSION = 1.1

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SHORT_DATE_FORMAT = "%Y-%m-%d"
ONE_DAY = datetime.timedelta(days=1)
TODAY = datetime.datetime.today()
YESTERDAY = TODAY - ONE_DAY
NULL_DATE = datetime.datetime(1, 1, 1, 0, 0, 0, 0)


#
# Functions
#
def get_rocketchat_timestamp(in_date):
    """Take in a date and return it converted to a Rocket.Chat timestamp"""
    s = in_date.strftime(DATE_FORMAT)
    return s[:-4] + 'Z'

def get_attachment_name(message, attachment):
    return message['ts'] + '_' + attachment['title']

def get_attachments_dir(json_data):
    return get_output_name(json_data) + '_attachments/'

def get_output_name(json_data):
    filename = (json_data['date']
                + '-'
                + json_data['channel_type']
                + '-'
                + json_data['channel_name'])

    return filename


def get_output_filepath(output_dir, json_data, extension):
    filename = (get_output_name(json_data)
                + '.'
                + extension.lower())

    return os.path.join(output_dir, filename)

def remove_empty_links(text):
    text_lines = text.split("\n")
    new_text = []
    for line in text_lines:
        if line == '':
            new_text.append(line)
            continue

        line = re.sub(r'(\[ \]\(http[s]?\:\/\/\S+\))', r'', line)

        if line != '':
            new_text.append(line)
    
    return '\n'.join(new_text)

# def is_multiline_code_tag(markdown_line):
#     words = markdown_line.split(" ")
#     if words[0].startswith("```") and words[0][3:4] != "`":
#         return True
#     else:
#         return False

# def replace_heading_tags(markdown_line, md_tag, html_tags):
#     if markdown_line[:len(md_tag) + 1] == md_tag:
#         letters = list(markdown_line)
#         del letters[:md_tag_len + 1]
#         letters.insert(0, html_tags[0])
#         letters.append(html_tags[1])
#         return "".join(letters)
#     else:
#         return markdown_line

# def replace_tags(markdown_line, md_tag, html_tags):
#     tag_side = 0
#     md_tag_len = len(md_tag)
#     words = markdown_line.split(" ")
#     converted_words = []
#     word_pairs = []
#     word_pair = []
#     tag_pairs = []
#     tag_pair = []
#     for word_i, word in enumerate(words):
#         for i, char in enumerate(word):
#             if char == md_tag:
#                 word_pair.append(word_i)
#                 tag_pair.append(i)
#                 if len(tag_pair) == 2:
#                     word_pairs.append(word_pair)
#                     word_pair = []
#                     tag_pairs.append(tag_pair)
#                     tag_pair = []
#         # letters = list(word)
#         # i = 0
#         # while True:
#         #     if i >= len(letters):
#         #         break
#         #     try:
#         #         chars = letters[i:i+md_tag_len][0]
#         #     except IndexError:
#         #         break
#         #     if chars == md_tag:
#         #         word_pair.append(word_i)
#         #         tag_pair.append(i)
#         #         if len(tag_pair) == 2:
#         #             word_pairs.append(word_pair)
#         #             word_pair = []
#         #             tag_pairs.append(tag_pair)
#         #             tag_pair = []

#         #     i += 1
    
#     letter_words = [list(word) for word in words]
#     for word_pair, tag_pair in zip(word_pairs, tag_pairs):
#         if tag_pair[1] - tag_pair[0] < 1:
#             continue
        
#         for word_i, tag_i in zip(word_pair, tag_pair):
#             letter_word = letter_words[word_i]
#             del letter_word[tag_i]
#             letter_word.insert(tag_i, html_tags[tag_side])
#             tag_side = int(not tag_side)

#             #del words[word_i]
#             #words.insert(word_i, "".join(letter_word))
#     words = ["".join(word) for word in letter_words]
#     return " ".join(words)

# def convert_markdown_to_html(markdown):
#     escaped_markdown = html.escape(markdown)
#     converted_lines = []
#     multiline_code_tag_pairs = []
#     multiline_code_tag_pair = []
#     multiline_code_tag_side = 0
#     multiline_code_html_tags = ["<pre class='code-block'><code>", "</code></pre>"]
#     for i, line in enumerate(escaped_markdown.split("\n")):
#         if is_multiline_code_tag(line):
#             multiline_code_tag_pair.append(i)
#             if len(multiline_code_tag_pair) == 2:
#                 multiline_code_tag_pairs.extend(multiline_code_tag_pair)
#                 multiline_code_tag_pair = []

    
#     for i, line in enumerate(escaped_markdown.split("\n")):
#         if i in multiline_code_tag_pairs:
#             line = multiline_code_html_tags[multiline_code_tag_side]
#             multiline_code_tag_side = int(not multiline_code_tag_side)
#         else:
#             line = replace_heading_tags(line, "#", ["<h1>", "</h1>"])
#             line = replace_heading_tags(line, "##", ["<h2>", "</h2>"])
#             line = replace_heading_tags(line, "###", ["<h3>", "</h3>"])
#             line = replace_heading_tags(line, "####", ["<h4>", "</h4>"])
#             line = replace_heading_tags(line, "#####", ["<h5>", "</h5>"])
#             line = replace_heading_tags(line, "######", ["<h6>", "</h6>"])
#             line = replace_tags(line, "*", ["<b>", "</b>"])
#             line = replace_tags(line, "_", ["<i>", "</i>"])
#             line = replace_tags(line, "~", ["<s>", "</s>"])
#             line = replace_tags(line, "`", ["<code class='inline-code'>", "</code>"])
#             line = re.sub(r'(http[s]?\:\/\/\S+)', r'<a href="\1">\1</a>', line)

#         converted_lines.append(line)
    
#     html_string = "</br>".join(converted_lines)

#     return html_string

def handle_markdown_for_html(markdown):
    escaped_markdown = html.escape(markdown)
    converted_lines = []
    for line in escaped_markdown.split("\n"):
        line = re.sub(r'(http[s]?\:\/\/\S+)', r'<a href="\1">\1</a>', line)
        converted_lines.append(line)
    
    return "</br>".join(converted_lines)

def convert_html_data_to_html(html_data, messages_by_ids):
    html_string = ""
    def process_html_data(data):
        html_string = ""
        for element in data:
            if element['type'] == 'HTML':
                html_string += "<html>"
                html_string += process_html_data(element['value'])
                html_string += "</html>"
            elif element['type'] == 'HEAD':
                html_string += "<head>"
                html_string += process_html_data(element['value'])
                html_string += "</head>"
            elif element['type'] == 'STYLESHEET':
                html_string += "<style>"
                html_string += process_html_data(element['value'])
                html_string += "</style>"
            elif element['type'] == 'TITLE':
                html_string += "<title>"
                html_string += process_html_data(element['value'])
                html_string += "</title>"
            elif element['type'] == 'BODY':
                html_string += "<body>"
                html_string += process_html_data(element['value'])
                html_string += "</body>"
            elif element['type'] == 'MESSAGE':
                html_class = 'message'
                if element.get("main_channel_thread_message"):
                    html_class += ' ' + 'main-channel-thread-message'
                html_string += f"<div class='{html_class}'>" #1
                if element.get("main_channel_thread_message"):
                    og_message = messages_by_ids.get(element['tmid'], {'u': {'name': "Unknown"}, 'msg': f"Message ID {element['tmid']} not found"})
                    if not og_message["msg"]:
                        if og_message.get("attachments"):
                            og_message["msg"] = og_message["attachments"][0].get("description", "")
                    html_string += f"<p class='thread-reply'><i>In reply to, {html.escape(og_message['u']['name'])}: {html.escape(og_message['msg'])}</i></p>"
                message_pinned = " Pinned a message " if element.get("t") == "message_pinned" else " "
                html_string += f"<p class='message-title'><b>{html.escape(element['message_title']['user'])}</b>{message_pinned}<i>{html.escape(element['message_title']['timestamp'])}</i>"
                if element.get('editedAt'):
                    edit_type = "Message removed" if element.get("t") == "rm" else "Edited"
                    html_string += f"<span class='message-edit'>{edit_type} at {html.escape(element['editedAt'])} by {html.escape(element['editedBy']['username'])}</span>"
                html_string += "</p>"
                html_string += "<div class='message-body'>" #2
                html_string += process_html_data(element['md'])
                html_string += process_html_data(element['attachments'])
                if element['reactions']:
                    html_string += "<div class='reactions'>" #3
                    html_string += process_html_data(element['reactions'])
                    html_string += "</div>" #3
                if element['thread_messages']:
                    html_string += f"<div class='thread'><p class='thread-title'><i>Message Replies ({len(element['thread_messages'])}):</i></p>" #4
                    html_string += "<div class='thread-messages'>" #5
                    html_string += process_html_data(element['thread_messages'])
                    html_string += "</div>" #5
                    html_string += "</div>" #4
                html_string += "</div>" #2
                html_string += "</div>" #1
            elif element['type'] == 'IMAGE':
                html_string += f"<i>{html.escape(element['name'])}</i></br>"
                html_string += f"<img src='{html.escape(element['src'])}' width='{html.escape(str(element['width']))}'></img>"
            elif element['type'] == 'VIDEO':
                html_string += f"<i>{html.escape(element['name'])}</i></br>"
                html_string += f"<video src='{element['src']}' controls='true' width='512'></video>"
            elif element['type'] == 'AUDIO':
                html_string += f"<i>{html.escape(element['name'])}</i></br>"
                html_string += f"<audio src='{html.escape(element['src'])}' controls='true' width='512'></audio>"
            elif element['type'] == 'FILE':
                html_string += f"<i>{html.escape(element['name'])}</i>"
                html_string += "<div class='file-attachment'>"
                html_string += f"</br><a href='{html.escape(element['src'])}'>{html.escape(element['text'])}</a>"
                html_string += "</div>"
            elif element['type'] == 'MESSAGE_QUOTE':
                html_string += f"<div class='message-quote'>{html.escape('<QUOTE>')}</br><b>{html.escape(element['author_name'])}</b> <i>{html.escape(element['ts'])}</i></br><a href='{html.escape(element['src'])}'>{html.escape(element['src'])}</a>"
                if element.get("text"):
                    html_string += f"</br>{handle_markdown_for_html(element['text'])}"
                if element.get("subattachments"):
                    for subattachment in element["subattachments"]:
                        if subattachment.get('author_name') and subattachment.get('ts'):
                            html_string += "</br>"
                            html_string += f"</br><b>{html.escape(subattachment['author_name'])}</b> <i>{html.escape(subattachment['ts'])}</i>"
                        if subattachment.get('message_link'):
                            html_string += f"</br><a href='{html.escape(subattachment['message_link'])}'>{html.escape(subattachment['message_link'])}</a>"
                        if subattachment.get('text'):
                            html_string += "</br>"
                            html_string += handle_markdown_for_html(subattachment['text'])
                        if subattachment.get('title'):
                            html_string += "</br>" + html.escape(f"<ATTACHMENT>{subattachment['title']}</ATTACHMENT>")
                html_string += f"</br>{html.escape('</QUOTE>')}</div>"
            elif element['type'] == 'MESSAGE_PINNED':
                html_string += f"<div class='quote'><b>{html.escape(element['author_name'])}</b> <i>{html.escape(element['ts'])}</i>"
                if element.get("text"):
                    html_string += f"</br>{handle_markdown_for_html(element['text'])}"
                if element.get("subattachments"):
                    for subattachment in element["subattachments"]:
                        if subattachment.get('author_name') and subattachment.get('ts'):
                            html_string += "</br>"
                            html_string += f"</br><b>{html.escape(subattachment['author_name'])}</b> <i>{html.escape(subattachment['ts'])}</i>"
                        if subattachment.get('message_link'):
                            html_string += f"</br><a href='{html.escape(subattachment['message_link'])}'>{html.escape(subattachment['message_link'])}</a>"
                        if subattachment.get('text'):
                            html_string += "</br>"
                            html_string += handle_markdown_for_html(subattachment['text'])
                        if subattachment.get('title'):
                            html_string += "</br>" + html.escape(f"<ATTACHMENT>{subattachment['title']}</ATTACHMENT>")
                html_string += f"</div>"
            elif element['type'] == 'REACTION':
                html_string += "<div class='reaction'>"
                html_string += f"<p class='reaction-emoji-padding'><b><u>{html.escape(element['emoji'])}</u></b></br><span class='reacted-by'>Reacted by ({len(element['names'])})</span>"
                for username, name in zip(element['usernames'], element['names']):
                    html_string += f"</br>{html.escape(name)}@{html.escape(username)}"
                html_string += "</p>"
                html_string += "</div>"
            elif element['type'] == 'STYLESHEET_STYLE':
                html_string += element['value']


            elif element['type'] == 'PLAIN_TEXT':
                html_string += html.escape(element['value'])
            elif element['type'] == 'PARAGRAPH':
                html_string += "<p>"
                html_string += process_html_data(element['value'])
                html_string += "</p>"
            elif element['type'] == 'MENTION_USER':
                html_string += f"<b>@{html.escape(element['value']['value'])}</b>"
            elif element['type'] == 'MENTION_CHANNEL':
                html_string += f"<b>#{html.escape(element['value']['value'])}</b>"
            elif element['type'] == 'INLINE_CODE':
                html_string += f"<code class='inline-code'>{html.escape(element['value']['value'])}</code>"
            elif element['type'] == 'CODE':
                html_string += "<pre class='code-block'><code>"
                html_string += process_html_data(element['value'])
                html_string += "</code></pre>"
            elif element['type'] == 'CODE_LINE':
                html_string += process_html_data([element['value']])
                html_string += "\n"
            elif element['type'] == 'EMOJI':
                if element.get('unicode'):
                    html_string += html.escape(element['unicode'])
                else:
                    html_string += f"<span class='emoji'><b><u>:{html.escape(element['shortCode'])}:</u></b></span>"
            elif element['type'] == 'BIG_EMOJI':
                html_string += "<span class='big-emoji'>"
                html_string += process_html_data(element['value'])
                html_string += "</span>"
            elif element['type'] == 'QUOTE':
                html_string += f"<div class='quote'><span class='quote-header'><b>QUOTE:</b></span>"
                html_string += process_html_data(element['value'])
                html_string += "</div>"
            elif element['type'] == 'LINK':
                link_src = element['value']['src']['value']
                if link_src.startswith("//"):
                    link_src = "https:" + link_src
                html_string += f"<a href='{html.escape(link_src)}' title='{html.escape(link_src)}' rel='noopener noreferrer' target='_blank'>"
                html_string += process_html_data(element['value']['label'])
                html_string += "</a>"
            elif element['type'] == 'BOLD':
                html_string += "<b>"
                html_string += process_html_data(element['value'])
                html_string += "</b>"
            elif element['type'] == 'ITALIC':
                html_string += "<i>"
                html_string += process_html_data(element['value'])
                html_string += "</i>"
            elif element['type'] == 'UNDERLINE': # Unsure if underlines are supported
                html_string += "<u>"
                html_string += process_html_data(element['value'])
                html_string += "</u>"
            elif element['type'] == 'STRIKE':
                html_string += "<s>"
                html_string += process_html_data(element['value'])
                html_string += "</s>"
            elif element['type'] == 'HEADING':
                html_string += f"<h{element['level']}>"
                html_string += process_html_data(element['value'])
                html_string += f"</{element['level']}>"
            elif element['type'] == 'LINE_BREAK':
                html_string += "</br>"
            elif element['type'] == 'UNORDERED_LIST':
                html_string += "<ul>"
                html_string += process_html_data(element['value'])
                html_string += "</ul>"
            elif element['type'] == 'ORDERED_LIST':
                html_string += "<ol type='none'>"
                html_string += process_html_data(element['value'])
                html_string += "</ol>"
            elif element['type'] == 'LIST_ITEM':
                html_string += "<li>"
                if element.get('number'):
                    html_string += f"<b>{html.escape(str(element['number']))}.</b> "
                html_string += process_html_data(element['value'])
                html_string += "</li>"


            else:
                html_string += element['value']

        return html_string

    return process_html_data([html_data])

def convert_json_to_html(output_dir, json_data):
    html_data = {
        "type": 'HTML',
        "value": [
            {
                "type": 'HEAD',
                "value": [
                    {
                        "type": 'STYLESHEET',
                        "value": [
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": 'p {margin:0px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.message {padding-bottom:20px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.message-edit {font-size: 11;margin-left: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.message-body {padding-left:20px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.main-channel-thread-message {font-size-adjust: 0.45;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.reactions {margin-top: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.file-attachment {border-style: solid;border-width: 1px;width: fit-content;padding: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.quote {border: solid 1px #888;padding: 5px 50px;border-radius: 10px;padding: 5px;width: fit-content;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.quote-header {color: #888;margin-right: 8px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": 'div.quote > p {display: inline-block;color: #4f4f4f;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.message-quote {box-shadow: 2px 2px 5px;width: fit-content;border: solid 1px;padding: 5px 50px;border-radius: 10px;background-color: #ececec;margin-top: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.reaction {display:inline-block;border-style: solid;border-width: 1px;border-radius: 10%;margin-right: 10px;background-color: #fffec3;overflow-y: auto;max-height: 200px;vertical-align : top}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.reaction-emoji-padding {display:inline-block;padding: 10px;margin: 0px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.reacted-by {font-size: 13;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.inline-code {background-color: lightgray;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.code-block {background-color: lightgray;width: fit-content;padding: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.emoji {border-color: #000;border-width: 1px;border-style: solid;border-radius: 10px;padding-right: 5px;background-color: #fffec3;padding-left: 5px;padding-bottom: inherit;padding-top: inherit;font-size-adjust: 0.45;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.big-emoji {display: inline-block;padding-top: 10px;font-size: 40;padding-bottom: 10;margin-top: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.big-emoji > .emoji {margin-right: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.thread-messages {height: 300px;overflow-y: auto;width: 30%;border: solid 1px;padding: 10px;}'
                            },
                            {
                                "type": 'STYLESHEET_STYLE',
                                "value": '.thread-reply {font-size: 11;}'
                            }
                        ]
                    },
                    {
                        "type": 'TITLE',
                        "value": [
                            {
                                "type": 'PLAIN_TEXT',
                                "value": f"{json_data['channel_name']} {json_data['date']}"
                            }
                        ]
                    }
                ]
            },
            {
                "type": 'BODY',
                "value": []
            }
        ]
    }

    messages_by_ids = {}

    for message_request in reversed(json_data["requests"]):
        def process_messages(messages, anchor, thread):
            for message in messages:
                if message.get('_hidden'):
                    continue

                message_html = {
                    "type": 'MESSAGE',
                    "message_title": {
                            'user': f"{message['u']['name']}@{message['u']['username']}",
                            'timestamp': message['ts']
                        },
                    "md": [],
                    "attachments": [],
                    "reactions": [],
                    "thread_messages": [],
                }

                messages_by_ids[message["_id"]] = message

                if not thread and message.get("tmid"):
                    if not message.get("tshow"):
                        continue
                    message_html["main_channel_thread_message"] = True
                    message_html["tmid"] = message["tmid"]

                if message.get("editedAt"):
                    message_html["editedAt"] = message["editedAt"]
                    message_html["editedBy"] = message["editedBy"]
                if message.get("t"):
                    message_html["t"] = message["t"]
                if message.get("md"):
                    message_html["md"].extend(message["md"])
                if message.get("attachments"):
                    for attachment in message["attachments"]:
                        attachment_html = {
                        }
                        if attachment.get("descriptionMd"):
                            message_html["md"].extend(attachment["descriptionMd"])

                        if attachment.get('title_link'):
                            get_output_name(json_data)
                            attachments_dir = get_attachments_dir(json_data)
                            attachment_name = get_attachment_name(message, attachment)
                            if attachment.get("image_type"):
                                attachment_html['type'] = 'IMAGE'
                                attachment_html["src"] = urllib.parse.quote(os.path.join(attachments_dir, attachment_name))
                                attachment_html["width"] = attachment['image_dimensions']['width']
                                attachment_html["height"] = attachment['image_dimensions']['height']
                                attachment_html["name"] = attachment['title']
                            elif attachment.get("video_type"):
                                attachment_html['type'] = 'VIDEO'
                                attachment_html["src"] = urllib.parse.quote(os.path.join(attachments_dir, attachment_name))
                                attachment_html["name"] = attachment['title']
                            elif attachment.get("audio_type"):
                                attachment_html['type'] = 'AUDIO'
                                attachment_html["src"] = urllib.parse.quote(os.path.join(attachments_dir, attachment_name))
                                attachment_html["name"] = attachment['title']
                            else:
                                attachment_html['type'] = 'FILE'
                                attachment_html["src"] = urllib.parse.quote(os.path.join(attachments_dir, attachment_name))
                                attachment_html["text"] = os.path.join(attachments_dir, attachment_name)
                                attachment_html["name"] = attachment['title']

                        if attachment.get('message_link'):
                            attachment_html['type'] = 'MESSAGE_QUOTE'
                            attachment_html["ts"] = attachment['ts']
                            attachment_html["src"] = attachment['message_link']
                            attachment_html["author_name"] = attachment['author_name']
                            attachment_html["text"] = remove_empty_links(attachment['text'])
                            if attachment.get('attachments'):
                                attachment_html["subattachments"] = []
                                subattachments = attachment['attachments']
                                while subattachments:
                                    subattachment = subattachments[0]
                                    subattachment_html = {}
                                    if subattachment.get("text"):
                                        subattachment_html["text"] = remove_empty_links(subattachment['text'])
                                    if subattachment.get("author_name") and subattachment.get("ts"):
                                        subattachment_html["author_name"] = subattachment["author_name"]
                                        subattachment_html["ts"] = subattachment["ts"]
                                    if subattachment.get("message_link"):
                                        subattachment_html["message_link"] = subattachment["message_link"]
                                    if subattachment.get("title"):
                                        subattachment_html["title"] = subattachment["title"]
                                    # if subattachment.get("message_link"):
                                    #     if attachment_html["text"]:
                                    #         attachment_html["text"] += '\n'
                                    #     if subattachment.get("author_name"):
                                    #         attachment_html["text"] += f"*{subattachment['author_name']}*"
                                    #     if subattachment.get("ts"):
                                    #         attachment_html["text"] += f" _{subattachment['ts']}_\n"
                                    #     attachment_html["text"] += subattachment["message_link"]
                                    # if subattachment.get("text"):
                                    #     if attachment_html["text"]:
                                    #         attachment_html["text"] += '\n'
                                    #     subattachment_text = subattachment["text"].split("\n")
                                    #     new_subattachment_text = []
                                    #     for line in subattachment_text:
                                    #         if not line.startswith("[ ]"):
                                    #             new_subattachment_text.append(line)
                                    #     attachment_html["text"] += '\n'.join(new_subattachment_text)
                                    # if subattachment.get("title"):
                                    #     if attachment_html["text"]:
                                    #         attachment_html["text"] += '\n'
                                    #     attachment_html["text"] += subattachment["title"]
                                    attachment_html["subattachments"].append(subattachment_html)

                                    if subattachment.get("attachments"):
                                        subattachments.extend(subattachment["attachments"])
                                    
                                    del subattachments[0]

                        if message.get("t") == "message_pinned":
                            attachment_html['type'] = 'MESSAGE_PINNED'
                            attachment_html["ts"] = attachment['ts']
                            attachment_html["author_name"] = attachment['author_name']
                            attachment_html["text"] = remove_empty_links(attachment['text'])
                            if attachment.get('attachments'):
                                attachment_html["subattachments"] = []
                                subattachments = attachment['attachments']
                                while subattachments:
                                    subattachment = subattachments[0]
                                    subattachment_html = {}
                                    if subattachment.get("text"):
                                        subattachment_html["text"] = remove_empty_links(subattachment['text'])
                                    if subattachment.get("author_name") and subattachment.get("ts"):
                                        subattachment_html["author_name"] = subattachment["author_name"]
                                        subattachment_html["ts"] = subattachment["ts"]
                                    if subattachment.get("message_link"):
                                        subattachment_html["message_link"] = subattachment["message_link"]
                                    if subattachment.get("title"):
                                        subattachment_html["title"] = subattachment["title"]
                                    attachment_html["subattachments"].append(subattachment_html)

                                    if subattachment.get("attachments"):
                                        subattachments.extend(subattachment["attachments"])
                                    
                                    del subattachments[0]

                        message_html["attachments"].append(attachment_html)

                if message.get("reactions"):
                    for key, value in message["reactions"].items():
                        reaction_html = {
                            "type": 'REACTION',
                            "emoji": key,
                            "usernames": value["usernames"],
                            "names": value.get("names", [""] * len(value["usernames"]))
                        }
                        message_html["reactions"].append(reaction_html)

                if message.get("thread_requests"):
                    for thread_request in message["thread_requests"]:
                        process_messages(thread_request["messages"], message_html["thread_messages"], True)

                anchor.append(message_html)

        process_messages(reversed(message_request["messages"]), html_data["value"][1]["value"], False)

    filepath = get_output_filepath(output_dir, json_data, 'html')
    with open(filepath, 'w') as f:
        f.write(convert_html_data_to_html(html_data, messages_by_ids))

def assemble_state(state_array, room_json, room_type):
    """Build the state_array that tracks what needs to be saved"""
    for channel in room_json[room_type]:
        if channel['_id'] not in state_array:
            state_array[channel['_id']] = {
                'name': channel['name'] if 'name' in channel else 'direct-'+channel['_id'],
                'type': room_type,
                'lastsaved': NULL_DATE,
                'begintime': (datetime
                              .datetime
                              .strptime(channel['ts'], DATE_FORMAT)
                              .replace(hour=0, minute=0, second=0, microsecond=0))
            }
        # Channels without messages don't have a lm field
        if channel.get('lm'):
            lm = datetime.datetime.strptime(channel['lm'], DATE_FORMAT)
        else:
            lm = NULL_DATE
        state_array[channel['_id']]['lastmessage'] = lm


def upgrade_state_schema(state_array, old_schema_version, logger):
    """Modify the datain the saved state file as needed for new versions"""
    cur_schema_version = old_schema_version
    logger.info('State schema version of '
                + str(old_schema_version)
                + ' is less than current version of '
                + str(VERSION))
    if cur_schema_version < 1.1:
        logger.info('Upgrading ' + str(cur_schema_version) + ' to 1.1...')
        # 1.0->1.1 update values for 'type' key
        t_typemap = {'direct': 'ims', 'channel': 'channels'}
        for t_id in state_array:
            state_array[t_id]['type'] = t_typemap[state_array[t_id]['type']]
        state_array['_meta'] = {'schema_version': 1.1}
        logger.info('Finished ' + str(cur_schema_version) + ' to 1.1...')
        cur_schema_version = state_array['_meta']['schema_version']
        logger.debug('\n' + pprint.pformat(state_array))

def handle_request_error(history_data, polite_pause, logger):
    error_text = history_data['error']
    logger.error('Error response from API endpoint: %s', error_text)
    if 'error-too-many-requests' in error_text:
        seconds_search = re.search(r'must wait (\d+) seconds',
                                    error_text,
                                    re.IGNORECASE)
        if seconds_search:
            seconds_to_wait = int(seconds_search.group(1))
            if seconds_to_wait < 300:
                polite_pause += seconds_to_wait \
                if seconds_to_wait < polite_pause \
                else polite_pause
                logger.error('Attempting handle API rate limit error by \
                                sleeping for %d and updating polite_pause \
                                to %d for the duration of this execution',
                                seconds_to_wait, polite_pause)
                sleep(seconds_to_wait)
            else:
                raise Exception('Unresonable amount of time to wait '
                                + 'for API rate limit')
        else:
            raise Exception('Can not parse too-many-requests error message')
    else:
        raise Exception('Untrapped error response from history API: '
                        + '{error_text}'
                        .format(error_text=error_text))

    return polite_pause

def get_message_attachments(message, output_dir, rc_server, json_data):
    for attachment in message['attachments']:
        if attachment.get('title_link'):
            url = rc_server + attachment['title_link']
            rqst = requests.get(url, allow_redirects=True)
            attachments_dir = get_attachments_dir(json_data)
            try:
                os.mkdir(os.path.join(output_dir, attachments_dir))
            except FileExistsError:
                pass
            attachment_name = get_attachment_name(message, attachment)
            attachment_filepath = os.path.join(output_dir, attachments_dir, attachment_name)
            with open(attachment_filepath, 'wb') as f:
                f.write(rqst.content)


def get_thread_messages(rc_server, rocket, logger, count_max, polite_pause, output_dir, json_data, og_message):
    get_messages = True
    skip = 0
    message_requests = []

    while get_messages:
        logger.info('')
        logger.info('start thread: %s', og_message['tlm'])
        history_data_obj = {}
        retry_flag = True
        retry_count = 0

        while retry_flag:
            retry_count += 1
            logger.debug('invoking API to get thread messages (attempt %d)', retry_count)

            history_data_obj = rocket.call_api_get(
                "chat.getThreadMessages",
                count=count_max,
                offset=skip,
                tmid=og_message['_id'],
                tlm=og_message['tlm'])

            history_data = history_data_obj.json()

            if not history_data['success']:
                polite_pause = handle_request_error(history_data, polite_pause, logger)
            else:
                retry_flag = False

        num_messages = len(history_data['messages'])
        logger.info('Thread messages found: %s', str(num_messages))

        if num_messages > 0:
            message_requests.append(history_data)
            for message in history_data['messages']:
                if message.get('attachments'):
                    get_message_attachments(message,
                                            output_dir,
                                            rc_server,
                                            json_data)

            skip += num_messages
            sleep(polite_pause)

        else:
            skip = 0
            get_messages = False

    og_message['thread_requests'] = message_requests

    return polite_pause

#
# Main
#
def main():
    """Main export process"""
    # args
    argparser_main = argparse.ArgumentParser()
    argparser_main.add_argument('configfile',
                                help='Location of configuration file')
    argparser_main.add_argument('-s', '--datestart',
                                help='Datetime to use for global starting point ' + \
                                'e.g. 2016-01-01 (implied T00:00:00.000Z)')
    argparser_main.add_argument('-e', '--dateend',
                                help='Datetime to use for global ending point ' + \
                                'e.g. 2016-01-01 (implied T23:59:59.999Z)')
    argparser_main.add_argument('--convert',
                                help='Convert JSON files to HTML files after download.',
                                action="store_true")
    argparser_main.add_argument('--convert-only',
                                help='Convert already downloaded JSON files to HTML ' + \
                                'files within the specified date range. Doesn\'t ' + \
                                'retrieve messages.',
                                action="store_true")
    argparser_main.add_argument('-r', '--readonlystate',
                                help='Do not create or update history state file.',
                                action="store_true")
    args = argparser_main.parse_args()

    if args.convert and args.convert_only:
        return print("Error: --convert and --convert-only conflict.  Pick one or the other.")

    start_time = (datetime
                  .datetime
                  .strptime(args.datestart,
                            SHORT_DATE_FORMAT)
                  .replace(hour=0,
                           minute=0,
                           second=0,
                           microsecond=0) if args.datestart else None)

    end_time = (datetime
                .datetime
                .strptime(args.dateend,
                          SHORT_DATE_FORMAT)
                .replace(hour=23,
                         minute=59,
                         second=59,
                         microsecond=999999) if args.dateend \
                         else YESTERDAY.replace(hour=23,
                                                minute=59,
                                                second=59,
                                                microsecond=999999))


    # config
    config_main = configparser.ConfigParser()
    config_main.read(args.configfile)

    polite_pause = int(config_main['rc-api']['pause_seconds'])
    count_max = int(config_main['rc-api']['max_msg_count_per_day'])
    output_dir = config_main['files']['history_output_dir']
    state_file = config_main['files']['history_statefile']

    rc_user = config_main['rc-api']['user']
    rc_pass = config_main['rc-api']['pass']
    rc_server = config_main['rc-api']['server']
    use_pat = config_main['rc-api'].getboolean('use_pat')


    # logging
    logger = logging.getLogger('export-history')
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler('export-history.log')
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False

    if args.convert_only:
        logger.info('BEGIN execution at %s', str(datetime.datetime.today()))
        for filename in os.listdir(output_dir):
            if filename.endswith(".json"):
                with open(os.path.join(output_dir, filename), "r") as f:
                    json_data = json.load(f)
                    if not json_data.get("date"):
                        logger.debug(f"Could not get date for file {filename}")
                        continue
                    json_date = (datetime
                                 .datetime
                                 .strptime(json_data["date"],
                                           SHORT_DATE_FORMAT)
                                 .replace(hour=0,
                                          minute=0,
                                          second=0,
                                          microsecond=0))

                    within_start_time = False
                    if start_time is not None:
                        if json_date >= start_time:
                            within_start_time = True
                    else:
                        within_start_time = True

                    within_end_time = False
                    if end_time is not None:
                        if json_date <= end_time:
                            within_end_time = True
                    else:
                        within_end_time = True

                    if within_start_time and within_end_time:
                        logger.info(f"Converting {filename} to HTML.")
                        convert_json_to_html(output_dir, json_data)

        logger.info('END execution at %s\n------------------------\n\n',
                str(datetime.datetime.today()))
        return

    room_state = {}

    logger.info('BEGIN execution at %s', str(datetime.datetime.today()))
    logger.debug('Command line arguments: %s', pprint.pformat(args))

    if args.readonlystate:
        logger.info('Running in readonly state mode. No state file updates.')

    if os.path.isfile(state_file):
        logger.debug('LOAD state from %s', state_file)
        sf = open(state_file, 'rb')
        room_state = pickle.load(sf)
        sf.close()
        logger.debug('\n%s', pprint.pformat(room_state))
        schema_version = 1.0 if '_meta' not in room_state else room_state['_meta']['schema_version']
        if schema_version < VERSION:
            upgrade_state_schema(room_state, schema_version, logger)

    else:
        logger.debug('No state file at %s, so state will be created', state_file)
        room_state = {'_meta': {'schema_version': VERSION}}


    logger.debug('Initialize rocket.chat API connection')
    if use_pat:
        rocket = RocketChat(user_id=rc_user, auth_token=rc_pass, server_url=rc_server)
    else:
        rocket = RocketChat(rc_user, rc_pass, server_url=rc_server)
    sleep(polite_pause)

    logger.debug('LOAD / UPDATE room state')
    assemble_state(room_state, rocket.channels_list_joined().json(), 'channels')
    sleep(polite_pause)

    assemble_state(room_state, rocket.im_list().json(), 'ims')
    sleep(polite_pause)

    assemble_state(room_state, rocket.groups_list().json(), 'groups')
    sleep(polite_pause)


    for channel_id, channel_data in room_state.items():
        if channel_id != '_meta':  # skip state metadata which is not a channel
            #TODO add support for filtering on channel name
            logger.info('------------------------')
            logger.info('Processing room: ' + channel_id + ' - ' + channel_data['name'])

            logger.debug('Global start time: %s', str(start_time))
            logger.debug('Global end time: %s', str(end_time))
            logger.debug('Room start ts: %s', str(channel_data['begintime']))
            logger.debug('Last message: %s', str(channel_data['lastmessage']))
            logger.debug('Last saved: %s ', str(channel_data['lastsaved']))

            if start_time is not None:
                # use globally specified start time but if the start time
                # is before the channel existed, fast-forward to its creation
                t_oldest = channel_data['begintime'] if channel_data['begintime'] > start_time \
                else start_time
            elif channel_data['lastsaved'] != NULL_DATE:
                # no global override for start time, so use a tick after
                # the last saved date if it exists
                t_oldest = channel_data['lastsaved'] + datetime.timedelta(microseconds=1)
            else:
                # nothing specified at all so use the beginning time of the channel
                t_oldest = channel_data['begintime']

            t_latest = NULL_DATE

            if (t_oldest < end_time) and (t_oldest < channel_data['lastmessage']):
                logger.info('Grabbing messages since '
                            + str(t_oldest)
                            + ' through '
                            + str(end_time))
            else:
                logger.info('Nothing to grab between '
                            + str(t_oldest)
                            + ' through '
                            + str(end_time))

            skip = 0

            day_has_messages = False
            message_requests = {
                "channel_name": channel_data['name'],
                "channel_type": channel_data['type'],
                "date": t_oldest.strftime('%Y-%m-%d'),
                "requests": []
                }


            while (t_oldest < end_time) and (t_oldest < channel_data['lastmessage']):
                logger.info('')
                t_latest = t_oldest + ONE_DAY - datetime.timedelta(microseconds=1)
                logger.info('start: %s', get_rocketchat_timestamp(t_oldest))

                history_data_obj = {}
                retry_flag = True
                retry_count = 0

                while retry_flag:
                    retry_count += 1
                    logger.debug('invoking API to get messages (attempt %d)', retry_count)
                    if channel_data['type'] == 'channels':
                        history_data_obj = rocket.channels_history(
                            channel_id,
                            count=count_max,
                            offset=skip,
                            inclusive='true',
                            latest=get_rocketchat_timestamp(t_latest),
                            oldest=get_rocketchat_timestamp(t_oldest))
                    elif channel_data['type'] == 'ims':
                        history_data_obj = rocket.im_history(
                            channel_id,
                            count=count_max,
                            offset=skip,
                            inclusive='true',
                            latest=get_rocketchat_timestamp(t_latest),
                            oldest=get_rocketchat_timestamp(t_oldest))
                    elif channel_data['type'] == 'groups':
                        history_data_obj = rocket.groups_history(
                            channel_id,
                            count=count_max,
                            offset=skip,
                            inclusive='true',
                            latest=get_rocketchat_timestamp(t_latest),
                            oldest=get_rocketchat_timestamp(t_oldest))

                    history_data = history_data_obj.json()

                    if not history_data['success']:
                        polite_pause = handle_request_error(history_data, polite_pause, logger)
                    else:
                        retry_flag = False


                num_messages = len(history_data['messages'])
                logger.info('Messages found: %s', str(num_messages))

                if num_messages > 0:
                    day_has_messages = True
                    message_requests['requests'].append(history_data)
                    for message in history_data['messages']:
                        if message.get('attachments'):
                            get_message_attachments(
                                message,
                                output_dir,
                                rc_server,
                                message_requests)

                        if message.get('tlm'):
                            polite_pause = get_thread_messages(
                                rc_server,
                                rocket,
                                logger,
                                count_max,
                                polite_pause,
                                output_dir,
                                message_requests,
                                message)

                    skip += num_messages
                    sleep(polite_pause)

                else: # Finished getting messages for the day
                    skip = 0
                    if day_has_messages:
                        filepath = get_output_filepath(output_dir, message_requests, 'json')
                        with open(filepath, 'w') as f:
                            json.dump(message_requests, f)

                        if args.convert:
                            logger.info(f"Converting {filepath} to HTML")
                            convert_json_to_html(output_dir, message_requests)

                    day_has_messages = False


                    logger.info('end: %s', get_rocketchat_timestamp(t_latest))
                    logger.info('')
                    t_oldest += ONE_DAY
                    sleep(polite_pause)

                    message_requests = {
                        "channel_name": channel_data['name'],
                        "channel_type": channel_data['type'],
                        "date": t_oldest.strftime('%Y-%m-%d'),
                        "requests": []
                        }

            logger.info('------------------------\n')

        # I am changing what 'lastsaved' means here. It used to denote the
        # last time a file was actually saved to disk for this channel
        # but I think it is more useful if it represents the maximum time for
        # which the channel has been checked. this will reduce lots
        # of unnecessary day checks if a channel is dormant for a while and then
        # suddenly has a message in it. This is only helpful if the
        # history export script is run on a periodic basis.
        room_state[channel_id]['lastsaved'] = end_time

    if not args.readonlystate:
        logger.debug('UPDATE state file')
        logger.debug('\n%s', pprint.pformat(room_state))
        sf = open(state_file, 'wb')
        pickle.dump(room_state, sf)
        sf.close()
    else:
        logger.debug('Running in readonly state mode: SKIP updating state file')

    logger.info('END execution at %s\n------------------------\n\n',
                str(datetime.datetime.today()))

if __name__ == "__main__":
    main()
