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
 * Copies all of the mappings from the specified entry list to this map, including metadata.
 * The implementation uses MergeOperation with PassThroughMergePolicy, so the effect of 
 * this call is equivalent to synchronizing given entries using WAN replication.
 * Please note that all the keys in the request should belong to the partition id to which this request is being sent, all keys
 * matching to a different partition id shall be ignored. The API implementation using this request may need to send multiple
 * of these request messages for filling a request for a key set if the keys belong to different partitions.
 */
@SuppressWarnings("unused")
@Generated("9c0c70477a67f1bd2055e3617f804497")
public final class MapPutAllWithMetadataCodec {
    //hex: 0x014900
    public static final int REQUEST_MESSAGE_TYPE = 84224;
    //hex: 0x014901
    public static final int RESPONSE_MESSAGE_TYPE = 84225;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private MapPutAllWithMetadataCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of map
         */
        public java.lang.String name;

        /**
         * entries with metadata
         */
        public java.util.List<com.hazelcast.map.impl.SimpleEntryView<com.hazelcast.internal.serialization.Data, com.hazelcast.internal.serialization.Data>> entries;
    }

    public static ClientMessage encodeRequest(java.lang.String name, java.util.Collection<com.hazelcast.map.impl.SimpleEntryView<com.hazelcast.internal.serialization.Data, com.hazelcast.internal.serialization.Data>> entries) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Map.PutAllWithMetadata");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        ListMultiFrameCodec.encode(clientMessage, entries, SimpleEntryViewCodec::encode);
        return clientMessage;
    }

    public static MapPutAllWithMetadataCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        //empty initial frame
        iterator.next();
        request.name = StringCodec.decode(iterator);
        request.entries = ListMultiFrameCodec.decode(iterator, SimpleEntryViewCodec::decode);
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
