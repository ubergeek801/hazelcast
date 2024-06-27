/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;

import javax.annotation.Nullable;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 * Applies given MC config (client filter list).
 */
@SuppressWarnings("unused")
@Generated("58f48dcb7d443c2a5031f48e6bb84608")
public final class MCApplyMCConfigCodec {
    //hex: 0x200D00
    public static final int REQUEST_MESSAGE_TYPE = 2100480;
    //hex: 0x200D01
    public static final int RESPONSE_MESSAGE_TYPE = 2100481;
    private static final int REQUEST_CLIENT_BW_LIST_MODE_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_CLIENT_BW_LIST_MODE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private MCApplyMCConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * ETag value of the config.
         */
        public java.lang.String eTag;

        /**
         * The mode for client filtering:
         * 0 - DISABLED
         * 1 - WHITELIST
         * 2 - BLACKLIST
         */
        public int clientBwListMode;

        /**
         * Client filter list entries.
         */
        public java.util.List<com.hazelcast.internal.management.dto.ClientBwListEntryDTO> clientBwListEntries;
    }

    public static ClientMessage encodeRequest(java.lang.String eTag, int clientBwListMode, java.util.Collection<com.hazelcast.internal.management.dto.ClientBwListEntryDTO> clientBwListEntries) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("MC.ApplyMCConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_CLIENT_BW_LIST_MODE_FIELD_OFFSET, clientBwListMode);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, eTag);
        ListMultiFrameCodec.encode(clientMessage, clientBwListEntries, ClientBwListEntryCodec::encode);
        return clientMessage;
    }

    public static MCApplyMCConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.clientBwListMode = decodeInt(initialFrame.content, REQUEST_CLIENT_BW_LIST_MODE_FIELD_OFFSET);
        request.eTag = StringCodec.decode(iterator);
        request.clientBwListEntries = ListMultiFrameCodec.decode(iterator, ClientBwListEntryCodec::decode);
        return request;
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }
}
