import { ClusterAdapter, ClusterMessage, MessageType } from "./cluster-adapter";
import { decode, encode } from "notepack.io";
import { hasBinary } from "./util";
import debugModule from "debug";

const debug = debugModule("socket.io-redis");

const RETURN_BUFFERS = true;

export interface ShardedRedisAdapterOptions {
  channelPrefix?: string;
}

export function createShardedAdapter(
  pubClient: any,
  subClient: any,
  opts?: ShardedRedisAdapterOptions
) {
  return function (nsp) {
    return new ShardedRedisAdapter(nsp, pubClient, subClient, opts);
  };
}

function sidOf(opts) {
  return opts.rooms.at(0);
}

class ShardedRedisAdapter extends ClusterAdapter {
  private readonly pubClient: any;
  private readonly subClient: any;
  private readonly opts: Required<ShardedRedisAdapterOptions>;
  private readonly channel: string;
  private readonly responseChannel: string;
  private readonly cleanup: () => void;

  // by sorilove --------------->
  private listener = (message, channel) => this.onRawMessage(message, channel);

  private subscribings = new Set<string>();
  private sidsBy = new Map<string, Set<string>>();
  private roomsBy = new Map<string, Set<string>>();

  private channelOf(room: string) {
    return `${this.channel}${room}#`;
  }

  protected postjoin(message: { opts, rooms }) {
    const sid = sidOf(message.opts);
    if (!sid) {
      return;
    }

    message.rooms.forEach(room => {
      let rooms = this.roomsBy.get(sid);
      if (rooms === undefined) {
        this.roomsBy.set(sid, rooms = new Set());
      }
      rooms.add(room);
      let sids = this.sidsBy.get(room);
      if (sids === undefined) {
        this.sidsBy.set(room, sids = new Set());
      }
      sids.add(sid);
      const channel = this.channelOf(room);
      if (!this.subscribings.has(channel)) {
        console.log(`postjoin - ssubscribe ${channel}`);
        this.subClient.sSubscribe(channel, this.listener, RETURN_BUFFERS);
        this.subscribings.add(channel);
      }
    });
  }

  protected postleave(message: { opts, rooms }) {
    const tryunsubscribe = (room: string, sid: string) => {
      const sids = this.sidsBy.get(room);
      if (sids === undefined) {
        return;
      }
      sids.delete(sid);
      if (sids.size === 0) {
        this.sidsBy.delete(room);
        const channel = this.channelOf(room);
        if (this.subscribings.has(channel)) {
          console.log(`postjoin - sUnsubscribe ${channel}`);
          this.subClient.sUnsubscribe(channel, this.listener);
          this.subscribings.delete(channel);
        }
      }
    };

    const sid = sidOf(message.opts);
    if (!sid) {
      return;
    }

    const rooms = this.roomsBy.get(sid);
    if (rooms === undefined) {
      return;
    }

    const socket = this.nsp.sockets.get(sid);
    if (socket !== undefined) {
      message.rooms.forEach(room => {
        rooms.delete(room);
        tryunsubscribe(room, sid);
      });
    }
    else {
      rooms.forEach(room => {
        tryunsubscribe(room, sid);
      });
      this.roomsBy.delete(sid);
    }

  }

  protected postdisconnect(message: { opts, rooms }) {
    message.rooms.forEach(room => {
      const channel = this.channelOf(room);
      if (this.subscribings.has(channel)) {
        this.subClient.unsubscribe(channel);
        this.subscribings.delete(channel);
      }
    });
  }
  // by sorilove <---------------

  constructor(nsp, pubClient, subClient, opts: ShardedRedisAdapterOptions) {
    super(nsp);
    this.pubClient = pubClient;
    this.subClient = subClient;
    this.opts = Object.assign(
      {
        channelPrefix: "socket.io",
      },
      opts
    );

    this.channel = `${this.opts.channelPrefix}#${nsp.name}#`;
    this.responseChannel = `${this.opts.channelPrefix}#${nsp.name}#${this.uid}#`;

    const handler = (message, channel) => this.onRawMessage(message, channel);

    this.subClient.sSubscribe(this.channel, handler, RETURN_BUFFERS);
    this.subClient.sSubscribe(this.responseChannel, handler, RETURN_BUFFERS);

    this.cleanup = () => {
      return Promise.all([
        this.subClient.sUnsubscribe(this.channel, handler),
        this.subClient.sUnsubscribe(this.responseChannel, handler),
      ]);
    };
  }

  override close(): Promise<void> | void {
    this.cleanup();
  }

  override publishMessage(message) {
    debug("publishing message of type %s to %s", message.type, this.channel);

    const channel = message.data.opts.rooms?.length === 1 ?
      this.channelOf(message.data.opts.rooms.at(0)) :
      this.channel;
    
    this.pubClient.sPublish(channel, this.encode(message));

    return Promise.resolve("");
  }

  override publishResponse(requesterUid, response) {
    debug("publishing response of type %s to %s", response.type, requesterUid);

    this.pubClient.sPublish(
      `${this.channel}${requesterUid}#`,
      this.encode(response)
    );
  }

  private encode(message: ClusterMessage) {
    const mayContainBinary = [
      MessageType.BROADCAST,
      MessageType.BROADCAST_ACK,
      MessageType.FETCH_SOCKETS_RESPONSE,
      MessageType.SERVER_SIDE_EMIT,
      MessageType.SERVER_SIDE_EMIT_RESPONSE,
    ].includes(message.type);

    if (mayContainBinary && hasBinary(message.data)) {
      return encode(message);
    } else {
      return JSON.stringify(message);
    }
  }

  private onRawMessage(rawMessage: Buffer, channel: Buffer) {
    let message;
    try {
      if (rawMessage[0] === 0x7b) {
        message = JSON.parse(rawMessage.toString());
      } else {
        message = decode(rawMessage);
      }
    } catch (e) {
      return debug("invalid format: %s", e.message);
    }

    if (channel.toString().startsWith(this.channel)) {
      this.onMessage(message, "");
    } else {
      this.onResponse(message);
    }
  }

  override serverCount(): Promise<number> {
    if (
      this.pubClient.constructor.name === "Cluster" ||
      this.pubClient.isCluster
    ) {
      return Promise.all(
        this.pubClient.nodes().map((node) => {
          node.sendCommand(["PUBSUB", "SHARDNUMSUB", this.channel]);
        })
      ).then((values) => {
        let numSub = 0;
        values.map((value) => {
          numSub += parseInt(value[1], 10);
        });
        return numSub;
      });
    } else {
      return this.pubClient
        .sendCommand(["PUBSUB", "SHARDNUMSUB", this.channel])
        .then((res) => parseInt(res[1], 10));
    }
  }
}
