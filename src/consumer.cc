/* UPA consumer.
 */

#include "consumer.hh"

#include <algorithm>
#include <utility>

#include <windows.h>

#include <rtr/rsslPayloadEntry.h>

#include "chromium/logging.hh"
#include "upaostream.hh"
#include "client.hh"


#define MAX_MSG_SIZE 4096

/* Reuters Wire Format nomenclature for RDM dictionary names. */
static const std::string kRdmFieldDictionaryName ("RWFFld");
static const std::string kEnumTypeDictionaryName ("RWFEnum");

kinkan::consumer_t::consumer_t (
	const kinkan::config_t& config,
	std::shared_ptr<kinkan::upa_t> upa,
	Delegate* delegate 
	) :
	creation_time_ (boost::posix_time::second_clock::universal_time()),
	last_activity_ (creation_time_),
	config_ (config),
	upa_ (upa),
	delegate_ (delegate),
	connection_ (nullptr),
	is_muted_ (true),
	keep_running_ (true),
	service_id_ (1),	// first and only service
	cache_handle_ (nullptr),
	refresh_count_ (0),
	in_sync_ (false),
	pending_trigger_ (true)
{
	ZeroMemory (cumulative_stats_, sizeof (cumulative_stats_));
	ZeroMemory (snap_stats_, sizeof (snap_stats_));
}

kinkan::consumer_t::~consumer_t()
{
	DLOG(INFO) << "~consumer_t";
	Close();
/* Cleanup RSSL stack. */
	upa_.reset();
/* Summary output */
	using namespace boost::posix_time;
	auto uptime = second_clock::universal_time() - creation_time_;
	VLOG(3) << "Consumer summary: {"
		 " \"Uptime\": \"" << to_simple_string (uptime) << "\""
		", \"ConnectionsInitiated\": " << cumulative_stats_[CONSUMER_PC_CONNECTION_INITIATED] <<
		", \"ClientSessions\": " << cumulative_stats_[CONSUMER_PC_CLIENT_SESSION_ACCEPTED] <<
		", \"MsgsReceived\": " << cumulative_stats_[CONSUMER_PC_RSSL_MSGS_RECEIVED] <<
		", \"MsgsMalformed\": " << cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED] <<
		", \"MsgsSent\": " << cumulative_stats_[CONSUMER_PC_RSSL_MSGS_SENT] <<
		", \"MsgsEnqueued\": " << cumulative_stats_[CONSUMER_PC_RSSL_MSGS_ENQUEUED] <<
		" }";
}

/* Verify UPA library version.
 */
bool
kinkan::consumer_t::Initialize()
{
	RsslRet rc;

	last_activity_ = boost::posix_time::second_clock::universal_time();

/* RSSL Version Info. */
	if (!upa_->VerifyVersion())
		return false;

/* Last value cache, dictionary set as retrieved from upsteam source. */
	rc = rsslPayloadCacheInitialize();
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << "rsslPayloadCacheInitialize: { "
			  "\"rsslErrorId\": " << rc << ""
			" }";
		return false;
	}

	return true;
}

void
kinkan::consumer_t::Close()
{
/* Close all RSSL connections. */
	if (nullptr != connection_) {
		VLOG(3) << "Closing connection.";
		Close (connection_);
		connection_ = nullptr;
	}
/* Last value cache. */
	if (nullptr != cache_handle_) {
		rsslPayloadCacheDestroy (cache_handle_);
		cache_handle_ = nullptr;
	}
	if (rsslPayloadCacheIsInitialized())
		rsslPayloadCacheUninitialize();
	VLOG(3) << "Consumer closed.";
}

void
kinkan::consumer_t::Run()
{
	DLOG(INFO) << "Run";
	DCHECK(keep_running_) << "Quit must have been called outside of Run!";

	FD_ZERO (&in_rfds_); FD_ZERO (&out_rfds_);
	FD_ZERO (&in_wfds_); FD_ZERO (&out_wfds_);
	FD_ZERO (&in_efds_); FD_ZERO (&out_efds_);
	in_nfds_ = out_nfds_ = 0;
	in_tv_.tv_sec = 0;
	in_tv_.tv_usec = 1000 * 100;

	for (;;) {
		bool did_work = DoWork();

		if (!keep_running_)
			break;

		if (did_work)
			continue;

/* Reset fd state */
		out_rfds_ = in_rfds_;
		out_wfds_ = in_wfds_;
		out_efds_ = in_efds_;
		out_tv_.tv_sec = in_tv_.tv_sec;
		out_tv_.tv_usec = in_tv_.tv_usec;

		out_nfds_ = select (in_nfds_ + 1, &out_rfds_, &out_wfds_, &out_efds_, &out_tv_);
	}

	keep_running_ = true;
}

bool
kinkan::consumer_t::DoWork()
{
	DVLOG(3) << "DoWork";
	bool did_work = false;

	last_activity_ = boost::posix_time::second_clock::universal_time();

/* Only check keepalives on timeout */
	if (out_nfds_ <= 0
		&& nullptr != connection_)
	{
		RsslChannel* c = connection_;
		DVLOG(3) << "timeout, state " << internal::channel_state_string (c->state);
		if (RSSL_CH_STATE_ACTIVE == c->state) {
			if (last_activity_ >= NextPing()) {
				Ping (c);
			}
			if (last_activity_ >= NextPong()) {
				cumulative_stats_[CONSUMER_PC_RSSL_PONG_TIMEOUT]++;
				LOG(ERROR) << "Pong timeout from peer, aborting connection.";
				Abort (c);
			}
		}
		if (FD_ISSET (c->socketId, &out_efds_)) {
			cumulative_stats_[CONSUMER_PC_CONNECTION_EXCEPTION]++;
			DVLOG(3) << "Socket exception.";
/* Erase connection */
			connection_ = nullptr;
/* Remove RSSL socket from further event notification */
			FD_CLR (c->socketId, &in_rfds_);
			FD_CLR (c->socketId, &in_wfds_);
			FD_CLR (c->socketId, &in_efds_);
/* Ensure RSSL has closed out */
			if (RSSL_CH_STATE_CLOSED != c->state)
				Close (c);
		}
/* tbd: time based trigger. */
		if (pending_trigger_) {
static const boost::posix_time::time_duration target_time (11 + 5 - 1, 0, 15 /* lag */, 0);
			const boost::posix_time::time_duration now = last_activity_.time_of_day();
			if (now >= target_time) {
				delegate_->OnTrigger();
				pending_trigger_ = false;
			}
		}
		
		return false;
	}

/* Client connection */
	if (nullptr == connection_) {
		Connect();
		did_work = true;
	}

	if (nullptr != connection_) {
		RsslChannel* c = connection_;
/* incoming */
		if (FD_ISSET (c->socketId, &out_rfds_)) {
			FD_CLR (c->socketId, &out_rfds_);
			OnCanReadWithoutBlocking (c);
			did_work = true;
		}
/* outgoing */
		if (FD_ISSET (c->socketId, &out_wfds_)) {
			FD_CLR (c->socketId, &out_wfds_);
			OnCanWriteWithoutBlocking (c);
			did_work = true;
		}
/* Keepalive timeout on active session above connection */
		if (RSSL_CH_STATE_ACTIVE == c->state) {
			if (last_activity_ >= NextPing()) {
				Ping (c);
			}
			if (last_activity_ >= NextPong()) {
				cumulative_stats_[CONSUMER_PC_RSSL_PONG_TIMEOUT]++;
				LOG(ERROR) << "Pong timeout from peer, aborting connection.";
				Abort (c);
			}
		}
/* disconnects */
		if (FD_ISSET (c->socketId, &out_efds_)) {
			cumulative_stats_[CONSUMER_PC_CONNECTION_EXCEPTION]++;
			DVLOG(3) << "Socket exception.";
/* Erase connection */
			connection_ = nullptr;
/* Remove RSSL socket from further event notification */
			FD_CLR (c->socketId, &in_rfds_);
			FD_CLR (c->socketId, &in_wfds_);
			FD_CLR (c->socketId, &in_efds_);
/* Ensure RSSL has closed out */
			if (RSSL_CH_STATE_CLOSED != c->state)
				Close (c);
/* We want to hit select for timeout before reconnecting. */
			did_work = false;
		}
	}
	return did_work;
}

void
kinkan::consumer_t::Quit()
{
	keep_running_ = false;
}

/* 6.2. Establish Network Communication.
 * Create an outbound connection to the well-known hostname and port of an Interactive Provider. 
 */
void
kinkan::consumer_t::Connect()
{
#ifndef NDEBUG
	RsslConnectOptions addr = RSSL_INIT_CONNECT_OPTS;
#else
	RsslConnectOptions addr;
	rsslClearConnectOpts (&addr);
#endif
	RsslError rssl_err;

	DCHECK (nullptr == connection_);
	cumulative_stats_[CONSUMER_PC_CONNECTION_INITIATED]++;
	VLOG(2) << "Initiating new connection.";

	addr.connectionInfo.unified.address = const_cast<char*> (config_.rssl_server.c_str());
	addr.connectionInfo.unified.serviceName = const_cast<char*> (config_.upstream_rssl_port.c_str());
	addr.protocolType = RSSL_RWF_PROTOCOL_TYPE;
	addr.majorVersion = RSSL_RWF_MAJOR_VERSION;
	addr.minorVersion = RSSL_RWF_MINOR_VERSION;
	RsslChannel* c = rsslConnect (&addr, &rssl_err);
	if (nullptr == c) {
		LOG(ERROR) << "rsslConnect: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			", \"connectionInfo\": " << addr.connectionInfo << ""
			", \"protocolType\": " << addr.protocolType << ""
			", \"majorVersion\": " << addr.majorVersion << ""
			", \"minorVersion\": " << addr.minorVersion << ""
			" }";
	} else {
		connection_ = c;
/* Set logger ID */
		std::ostringstream ss;
		ss << c << ':';
		prefix_.assign (ss.str());

/* Wait for session */
		FD_SET (c->socketId, &in_rfds_);
		FD_SET (c->socketId, &in_wfds_);
		FD_SET (c->socketId, &in_efds_);

		cumulative_stats_[CONSUMER_PC_CONNECTION_ACCEPTED]++;

		LOG(INFO) << "RSSL socket created: { "
			  "\"connectionType\": \"" << internal::connection_type_string (c->connectionType) << "\""
			", \"majorVersion\": " << static_cast<unsigned> (c->majorVersion) << ""
			", \"minorVersion\": " << static_cast<unsigned> (c->minorVersion) << ""
			", \"pingTimeout\": " << c->pingTimeout << ""
			", \"protocolType\": \"" << internal::protocol_type_string (c->protocolType) << "\""
			", \"socketId\": " << c->socketId << ""
			", \"state\": \"" << internal::channel_state_string (c->state) << "\""
			" }";
	}
}

void
kinkan::consumer_t::OnCanReadWithoutBlocking (
	RsslChannel* c
	)
{
	DVLOG(3) << "OnCanReadWithoutBlocking";
	DCHECK (nullptr != c);
	switch (c->state) {
	case RSSL_CH_STATE_CLOSED:
		LOG(INFO) << "socket state is closed.";
/* Raise internal exception flags to remove socket */
		Abort (c);
		break;
	case RSSL_CH_STATE_INACTIVE:
		LOG(INFO) << "socket state is inactive.";
		break;
	case RSSL_CH_STATE_INITIALIZING:
		LOG(INFO) << "socket state is initializing.";
		break;
	case RSSL_CH_STATE_ACTIVE:
                OnActiveReadState (c);
		break;

	default:
		LOG(ERROR) << "socket state is unknown.";
		break;
	}
}

void
kinkan::consumer_t::OnInitializingState (
	RsslChannel* c
	)
{
	RsslInProgInfo state;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);

/* In place of absent API: rsslClearError (&rssl_err); */
	rssl_err.rsslErrorId = 0;
	rssl_err.sysError = 0;
	rssl_err.text[0] = '\0';

	rc = rsslInitChannel (c, &state, &rssl_err);
	switch (rc) {
	case RSSL_RET_CHAN_INIT_IN_PROGRESS:
		if ((state.flags & RSSL_IP_FD_CHANGE) == RSSL_IP_FD_CHANGE) {
			cumulative_stats_[CONSUMER_PC_RSSL_PROTOCOL_DOWNGRADE]++;
			LOG(INFO) << "RSSL protocol downgrade, reconnected.";
			FD_CLR (state.oldSocket, &in_rfds_); FD_SET (c->socketId, &in_rfds_);
			FD_CLR (state.oldSocket, &in_wfds_); FD_SET (c->socketId, &in_wfds_);
			FD_CLR (state.oldSocket, &in_efds_); FD_SET (c->socketId, &in_efds_);
		} else {
			LOG(INFO) << "RSSL connection in progress.";
		}
		break;
	case RSSL_RET_SUCCESS:
		OnActiveSession (c);
		FD_SET (c->socketId, &in_rfds_);
		FD_SET (c->socketId, &in_wfds_);
		FD_SET (c->socketId, &in_efds_);
		break;
	default:
		LOG(ERROR) << "rsslInitChannel: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
		break;
	}
}

void
kinkan::consumer_t::OnCanWriteWithoutBlocking (
	RsslChannel* c
	)
{
        DVLOG(3) << "OnCanWriteWithoutBlocking";
	DCHECK (nullptr != c);
        switch (c->state) {
        case RSSL_CH_STATE_CLOSED:
                LOG(INFO) << "socket state is closed.";
/* Raise internal exception flags to remove socket */
                Abort (c);
                break;
        case RSSL_CH_STATE_INACTIVE:
                LOG(INFO) << "socket state is inactive.";
                break;    
        case RSSL_CH_STATE_INITIALIZING:
                LOG(INFO) << "socket state is initializing.";
                OnInitializingState (c);
                break;
        case RSSL_CH_STATE_ACTIVE:
                OnActiveWriteState (c);
                break;

        default:
                LOG(ERROR) << "socket state is unknown.";
                break;
        }
}

void
kinkan::consumer_t::OnActiveWriteState (
	RsslChannel* c
	)
{
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);

/* In place of absent API: rsslClearError (&rssl_err); */
	rssl_err.rsslErrorId = 0;
	rssl_err.sysError = 0;
	rssl_err.text[0] = '\0';

	DVLOG(4) << "rsslFlush";
	rc = rsslFlush (c, &rssl_err);
	if (RSSL_RET_SUCCESS == rc) {
		cumulative_stats_[CONSUMER_PC_RSSL_FLUSH]++;
		FD_CLR (c->socketId, &in_wfds_);
/* Sent data equivalent to a ping. */
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_SENT] += GetPendingCount();
		ClearPendingCount();
		SetNextPing (last_activity_ + boost::posix_time::seconds (ping_interval_));
	} else if (rc > 0) {
		DVLOG(1) << static_cast<signed> (rc) << " bytes pending.";
	} else {
		LOG(ERROR) << "rsslFlush: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
}

void
kinkan::consumer_t::Abort (
	RsslChannel* c
	)
{
	DCHECK (nullptr != c);
	FD_CLR (c->socketId, &out_rfds_);
	FD_CLR (c->socketId, &out_wfds_);
	FD_SET (c->socketId, &out_efds_);
}

void
kinkan::consumer_t::Close (
	RsslChannel* c
	)
{
	RsslError rssl_err;

	DCHECK (nullptr != c);

	LOG(INFO) << "Closing RSSL connection.";
	if (RSSL_RET_SUCCESS != rsslCloseChannel (c, &rssl_err)) {
		LOG(WARNING) << "rsslCloseChannel: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
}

/* Handling Consumer Client Session Events: New client session request.
 *
 * There are many reasons why a consumer might reject a connection. For
 * example, it might have a maximum supported number of connections.
 */
bool
kinkan::consumer_t::OnActiveSession (
	RsslChannel* c
	)
{
	RsslChannelInfo info;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);
	last_activity_ = boost::posix_time::second_clock::universal_time();
	cumulative_stats_[CONSUMER_PC_OMM_ACTIVE_CLIENT_SESSION_RECEIVED]++;

/* Relog negotiated state. */
	LOG(INFO) << prefix_ <<
		  "RSSL negotiated state: { "
		  "\"connectionType\": \"" << internal::connection_type_string (c->connectionType) << "\""
		", \"majorVersion\": " << static_cast<unsigned> (c->majorVersion) << ""
		", \"minorVersion\": " << static_cast<unsigned> (c->minorVersion) << ""
		", \"pingTimeout\": " << c->pingTimeout << ""
		", \"protocolType\": \"" << internal::protocol_type_string (c->protocolType) << "\""
		", \"socketId\": " << c->socketId << ""
		", \"state\": \"" << internal::channel_state_string (c->state) << "\""
		" }";

/* Store negotiated Reuters Wire Format version information. */
	rc = rsslGetChannelInfo (c, &info, &rssl_err);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << "rsslGetChannelInfo: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
		return false;
	}

/* Log connected infrastructure. */
	std::stringstream components;
	components << "[ ";
	for (unsigned i = 0; i < info.componentInfoCount; ++i) {
		if (i > 0) components << ", ";
		components << "{ "
			"\"componentVersion\": \"" << std::string (info.componentInfo[i]->componentVersion.data, info.componentInfo[i]->componentVersion.length) << "\""
			" }";
	}
	components << " ]";

	LOG(INFO) << prefix_ <<
		  "channelInfo: { "
		  "\"clientToServerPings\": \"" << (info.clientToServerPings ? "true" : "false") << "\""
		", \"componentInfo\": " << components.str() << ""
		", \"compressionThreshold\": " << static_cast<unsigned> (info.compressionThreshold) << ""
		", \"compressionType\": \"" << internal::compression_type_string (info.compressionType) << "\""
		", \"guaranteedOutputBuffers\": " << static_cast<unsigned> (info.guaranteedOutputBuffers) << ""
		", \"maxFragmentSize\": " << static_cast<unsigned> (info.maxFragmentSize) << ""
		", \"maxOutputBuffers\": " << static_cast<unsigned> (info.maxOutputBuffers) << ""
		", \"numInputBuffers\": " << static_cast<unsigned> (info.numInputBuffers) << ""
		", \"pingTimeout\": " << static_cast<unsigned> (info.pingTimeout) << ""
/* null terminated but max length RSSL_RSSL_MAX_FLUSH_STRATEGY, not well documented. */
		", \"priorityFlushStrategy\": \"" << info.priorityFlushStrategy << "\""
		", \"serverToClientPings\": " << (info.serverToClientPings ? "true" : "false") << ""
		", \"sysRecvBufSize\": " << static_cast<unsigned> (info.sysRecvBufSize) << ""
		", \"sysSendBufSize\": " << static_cast<unsigned> (info.sysSendBufSize) << ""
		", \"tcpRecvBufSize\": " << static_cast<unsigned> (info.tcpRecvBufSize) << ""
		", \"tcpSendBufSize\": " << static_cast<unsigned> (info.tcpSendBufSize) << ""
		" }";
/* First token aka stream id */
	token_ = 1;
/* Derive expected RSSL ping interval from negotiated timeout. */
	ping_interval_ = c->pingTimeout / 3;
/* Schedule first RSSL ping. */
	next_ping_ = last_activity_ + boost::posix_time::seconds (ping_interval_);
/* Treat connect as first RSSL pong. */
	next_pong_ = last_activity_ + boost::posix_time::seconds (c->pingTimeout);
/* Reset RDM data dictionary and wait to request from upstream. */
	rsslClearDataDictionary (&rdm_dictionary_);
	return SendLoginRequest (c);
}

bool
kinkan::consumer_t::SendLoginRequest (
	RsslChannel* c
	)
{
/* TODO: convert to RsslRDMLoginRequest */
#ifndef NDEBUG
/* Static initialisation sets all fields rather than only the minimal set
 * required.  Use for debug mode and optimise for release builds.
 */
	RsslRequestMsg request = RSSL_INIT_REQUEST_MSG;
	RsslEncodeIterator it = RSSL_INIT_ENCODE_ITERATOR;
	RsslElementList	element_list = RSSL_INIT_ELEMENT_LIST;
	RsslElementEntry element_entry = RSSL_INIT_ELEMENT_ENTRY;
        RsslBuffer data_buffer = RSSL_INIT_BUFFER;
#else
	RsslRequestMsg request;
	RsslEncodeIterator it;
	RsslElementList	element_list;
	RsslElementEntry element_entry;
        RsslBuffer data_buffer;
	rsslClearRequestMsg (&request);
	rsslClearEncodeIterator (&it);
	rsslClearElementList (&element_list);
	rsslClearElementEntry (&element_entry);
        rsslClearBuffer (&data_buffer);
#endif
	RsslBuffer* buf;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);
	VLOG(2) << prefix_ << "Sending MMT_LOGIN request.";

/* Set the message model type. */
	request.msgBase.domainType = RSSL_DMT_LOGIN;
/* Set request type. */
	request.msgBase.msgClass = RSSL_MC_REQUEST;
	request.flags = RSSL_RQMF_STREAMING;
/* No payload. */
	request.msgBase.containerType = RSSL_DT_NO_DATA;
/* Set the login token. */
	request.msgBase.streamId = login_token_ = token_++;

/* In RFA lingo an attribute object */
        request.msgBase.msgKey.nameType    = RDM_LOGIN_USER_NAME;
        request.msgBase.msgKey.name.data   = const_cast<char*> (config_.user_name.c_str());
        request.msgBase.msgKey.name.length = static_cast<uint32_t> (config_.user_name.size());
        request.msgBase.msgKey.flags = RSSL_MKF_HAS_NAME_TYPE | RSSL_MKF_HAS_NAME;

/* Login Request Elements */
	request.msgBase.msgKey.attribContainerType = RSSL_DT_ELEMENT_LIST;
	request.msgBase.msgKey.flags |= RSSL_MKF_HAS_ATTRIB;

	buf = rsslGetBuffer (c, MAX_MSG_SIZE, RSSL_FALSE /* not packed */, &rssl_err);
	if (nullptr == buf) {
		LOG(ERROR) << prefix_ << "rsslGetBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			", \"size\": " << MAX_MSG_SIZE << ""
			", \"packedBuffer\": false"
			" }";
		return false;
	}
	rc = rsslSetEncodeIteratorBuffer (&it, buf);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorBuffer: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	rc = rsslSetEncodeIteratorRWFVersion (&it, c->majorVersion, c->minorVersion);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorRWFVersion: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"majorVersion\": " << static_cast<unsigned> (c->majorVersion) << ""
			", \"minorVersion\": " << static_cast<unsigned> (c->minorVersion) << ""
			" }";
		goto cleanup;
	}
	rc = rsslEncodeMsgInit (&it, reinterpret_cast<RsslMsg*> (&request), MAX_MSG_SIZE);
	if (RSSL_RET_ENCODE_MSG_KEY_OPAQUE != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeMsgInit: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"dataMaxSize\": " << MAX_MSG_SIZE << ""
			" }";
		goto cleanup;
	}

/* Encode attribute object after message instead of before as per RFA. */
	element_list.flags = RSSL_ELF_HAS_STANDARD_DATA;
	rc = rsslEncodeElementListInit (&it, &element_list, nullptr /* element id dictionary */, 9 /* count of elements */);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeElementListInit: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"flags\": \"RSSL_ELF_HAS_STANDARD_DATA\""
			" }";
		goto cleanup;
	}
/* Do not permit stale data, item stream should be closed. */
        static const uint64_t disallow_suspect_data = 0;
        element_entry.dataType  = RSSL_DT_UINT;
        element_entry.name      = RSSL_ENAME_ALLOW_SUSPECT_DATA;
        rc = rsslEncodeElementEntry (&it, &element_entry, &disallow_suspect_data);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_ALLOW_SUSPECT_DATA\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"allowSuspectData\": " << disallow_suspect_data << ""
                        " }";
                return false;
        }
/* Application id. */
        element_entry.dataType  = RSSL_DT_ASCII_STRING;
        element_entry.name      = RSSL_ENAME_APPID;
        data_buffer.data   = const_cast<char*> (config_.application_id.c_str());
        data_buffer.length = static_cast<uint32_t> (config_.application_id.size());
        rc = rsslEncodeElementEntry (&it, &element_entry, &data_buffer);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_APPNAME\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"applicationId\": " << config_.application_id << ""
                        " }";
                return false;
        }
/* Application name. */
        element_entry.dataType  = RSSL_DT_ASCII_STRING;
        element_entry.name      = RSSL_ENAME_APPNAME;
        data_buffer.data   = const_cast<char*> (config_.application_name.c_str());
        data_buffer.length = static_cast<uint32_t> (config_.application_name.size());
        rc = rsslEncodeElementEntry (&it, &element_entry, &data_buffer);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_APPNAME\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"applicationName\": " << config_.application_name << ""
                        " }";
                return false;
        }
/* Unique identifier per process on a particular host. */
        element_entry.dataType  = RSSL_DT_ASCII_STRING;
        element_entry.name      = RSSL_ENAME_INST_ID;
        data_buffer.data   = const_cast<char*> (config_.instance_id.c_str());
        data_buffer.length = static_cast<uint32_t> (config_.instance_id.size());
        rc = rsslEncodeElementEntry (&it, &element_entry, &data_buffer);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_INST_ID\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"instanceId\": " << config_.instance_id << ""
                        " }";
                return false;
        }
/* DACS position string. */
        element_entry.dataType  = RSSL_DT_ASCII_STRING;
        element_entry.name      = RSSL_ENAME_POSITION;
        data_buffer.data   = const_cast<char*> (config_.position.c_str());
        data_buffer.length = static_cast<uint32_t> (config_.position.size());
        rc = rsslEncodeElementEntry (&it, &element_entry, &data_buffer);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_POSITION\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"position\": " << config_.position << ""
                        " }";
                return false;
        }
/* Require DACS locks. */
	static const uint64_t provide_permission_expressions = 1;
	element_entry.dataType	= RSSL_DT_UINT;
	element_entry.name	= RSSL_ENAME_PROV_PERM_EXP;
	rc = rsslEncodeElementEntry (&it, &element_entry, &provide_permission_expressions);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"name\": \"RSSL_ENAME_PROV_PERM_EXP\""
			", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
			", \"providePermissionExpressions\": " << provide_permission_expressions << ""
			" }";
		goto cleanup;
	}
/* No proxy of permissions. */
        static const uint64_t disable_permission_profile = 0;
        element_entry.dataType  = RSSL_DT_UINT;
        element_entry.name      = RSSL_ENAME_PROV_PERM_PROF;
        rc = rsslEncodeElementEntry (&it, &element_entry, &disable_permission_profile);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_PROV_PERM_PROF\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"providePermissionProfile\": " << disable_permission_profile << ""
                        " }";
                goto cleanup;
        }
/* I am a consumer. */
        static const uint64_t consumer_role = RDM_LOGIN_ROLE_CONS;
        element_entry.dataType  = RSSL_DT_UINT;
        element_entry.name      = RSSL_ENAME_ROLE;
        rc = rsslEncodeElementEntry (&it, &element_entry, &consumer_role);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_PROV_PERM_PROF\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"providePermissionProfile\": " << consumer_role << ""
                        " }";
                goto cleanup;
        }
/* Provider drives stream recovery. */
        static const uint64_t single_open = 1;
        element_entry.dataType  = RSSL_DT_UINT;
        element_entry.name      = RSSL_ENAME_SINGLE_OPEN;
        rc = rsslEncodeElementEntry (&it, &element_entry, &single_open);
        if (RSSL_RET_SUCCESS != rc) {
                LOG(ERROR) << prefix_ << "rsslEncodeElementEntry: { "
                          "\"returnCode\": " << static_cast<signed> (rc) << ""
                        ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
                        ", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
                        ", \"name\": \"RSSL_ENAME_SINGLE_OPEN\""
                        ", \"dataType\": \"" << rsslDataTypeToString (element_entry.dataType) << "\""
                        ", \"singleOpen\": " << single_open << ""
                        " }";
                goto cleanup;
        }
	rc = rsslEncodeElementListComplete (&it, RSSL_TRUE /* commit */);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeElementListComplete: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	rc = rsslEncodeMsgKeyAttribComplete (&it, RSSL_TRUE /* commit */);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeMsgKeyAttribComplete: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	if (RSSL_RET_SUCCESS != rsslEncodeMsgComplete (&it, RSSL_TRUE /* commit */)) {
		LOG(ERROR) << prefix_ << "rsslEncodeMsgComplete: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	buf->length = rsslGetEncodedBufferLength (&it);
	LOG_IF(WARNING, 0 == buf->length) << prefix_ << "rsslGetEncodedBufferLength returned 0.";

	DLOG(INFO) << request;
/* Message validation: must use ASSERT libraries for error description :/ */
//	if (!rsslValidateMsg (reinterpret_cast<RsslMsg*> (&request))) {
//		cumulative_stats_[CONSUMER_PC_MMT_LOGIN_MALFORMED]++;
//		LOG(ERROR) << prefix_ << "rsslValidateMsg failed.";
//		goto cleanup;
//	} else {
//		cumulative_stats_[CONSUMER_PC_MMT_LOGIN_VALIDATED]++;
//		DVLOG(4) << prefix_ << "rsslValidateMsg succeeded.";
//	}

	if (!Submit (c, buf)) {
		goto cleanup;
	}
	cumulative_stats_[CONSUMER_PC_MMT_LOGIN_SENT]++;
	return true;
cleanup:
	cumulative_stats_[CONSUMER_PC_MMT_LOGIN_EXCEPTION]++;
	if (RSSL_RET_SUCCESS != rsslReleaseBuffer (buf, &rssl_err)) {
		LOG(WARNING) << prefix_ << "rsslReleaseBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
	return false;
}

/* TDB: submit resubscription with single service id to ignore service noise. */
bool
kinkan::consumer_t::SendDirectoryRequest (
	RsslChannel* c
	)
{
#ifndef NDEBUG
/* Static initialisation sets all fields rather than only the minimal set
 * required.  Use for debug mode and optimise for release builds.
 */
	RsslRequestMsg request = RSSL_INIT_REQUEST_MSG;
	RsslEncodeIterator it = RSSL_INIT_ENCODE_ITERATOR;
        RsslBuffer data_buffer = RSSL_INIT_BUFFER;
#else
	RsslRequestMsg request;
	RsslEncodeIterator it;
        RsslBuffer data_buffer;
	rsslClearRequestMsg (&request);
	rsslClearEncodeIterator (&it);
        rsslClearBuffer (&data_buffer);
#endif
	RsslBuffer* buf;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);
	VLOG(2) << prefix_ << "Sending MMT_DIRECTORY request.";

/* Set the message model type. */
	request.msgBase.domainType = RSSL_DMT_SOURCE;
/* Set request type. */
	request.msgBase.msgClass = RSSL_MC_REQUEST;
	request.flags = RSSL_RQMF_STREAMING;
/* No payload. */
	request.msgBase.containerType = RSSL_DT_NO_DATA;
/* Set the login token. */
	request.msgBase.streamId = token_;	/* login + 1 */

/* In RFA lingo an attribute object, TBD: group, load filters. */
	request.msgBase.msgKey.filter = RDM_DIRECTORY_SERVICE_INFO_FILTER	// service names
					| RDM_DIRECTORY_SERVICE_STATE_FILTER;	// up or down
        request.msgBase.msgKey.flags = RSSL_MKF_HAS_FILTER;

	buf = rsslGetBuffer (c, MAX_MSG_SIZE, RSSL_FALSE /* not packed */, &rssl_err);
	if (nullptr == buf) {
		LOG(ERROR) << prefix_ << "rsslGetBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			", \"size\": " << MAX_MSG_SIZE << ""
			", \"packedBuffer\": false"
			" }";
		return false;
	}
	rc = rsslSetEncodeIteratorBuffer (&it, buf);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorBuffer: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	rc = rsslSetEncodeIteratorRWFVersion (&it, c->majorVersion, c->minorVersion);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorRWFVersion: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"majorVersion\": " << static_cast<unsigned> (c->majorVersion) << ""
			", \"minorVersion\": " << static_cast<unsigned> (c->minorVersion) << ""
			" }";
		goto cleanup;
	}
	rc = rsslEncodeMsg (&it, reinterpret_cast<RsslMsg*> (&request));
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeMsgInit: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	buf->length = rsslGetEncodedBufferLength (&it);
	LOG_IF(WARNING, 0 == buf->length) << prefix_ << "rsslGetEncodedBufferLength returned 0.";

	DLOG(INFO) << request;
/* Message validation: must use ASSERT libraries for error description :/ */
	if (!rsslValidateMsg (reinterpret_cast<RsslMsg*> (&request))) {
		cumulative_stats_[CONSUMER_PC_MMT_DIRECTORY_MALFORMED]++;
		LOG(ERROR) << prefix_ << "rsslValidateMsg failed.";
		goto cleanup;
	} else {
		cumulative_stats_[CONSUMER_PC_MMT_DIRECTORY_VALIDATED]++;
		DVLOG(4) << prefix_ << "rsslValidateMsg succeeded.";
	}

	if (!Submit (c, buf)) {
		goto cleanup;
	}
	cumulative_stats_[CONSUMER_PC_MMT_DIRECTORY_SENT]++;
/* advance token counter only on success, re-use token on failure. */
	directory_token_ = token_++;
	return true;
cleanup:
	cumulative_stats_[CONSUMER_PC_MMT_DIRECTORY_EXCEPTION]++;
	if (RSSL_RET_SUCCESS != rsslReleaseBuffer (buf, &rssl_err)) {
		LOG(WARNING) << prefix_ << "rsslReleaseBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
	return false;
}

bool
kinkan::consumer_t::SendDictionaryRequest (
	RsslChannel* c,
	const uint16_t service_id,
	const std::string& dictionary_name
	)
{
#ifndef NDEBUG
/* Static initialisation sets all fields rather than only the minimal set
 * required.  Use for debug mode and optimise for release builds.
 */
	RsslRequestMsg request = RSSL_INIT_REQUEST_MSG;
	RsslEncodeIterator it = RSSL_INIT_ENCODE_ITERATOR;
        RsslBuffer data_buffer = RSSL_INIT_BUFFER;
#else
	RsslRequestMsg request;
	RsslEncodeIterator it;
        RsslBuffer data_buffer;
	rsslClearRequestMsg (&request);
	rsslClearEncodeIterator (&it);
        rsslClearBuffer (&data_buffer);
#endif
	RsslBuffer* buf;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);
	VLOG(2) << prefix_ << "Sending MMT_DICTIONARY request.";

/* Set the message model type. */
	request.msgBase.domainType = RSSL_DMT_DICTIONARY;
/* Set request type. */
	request.msgBase.msgClass = RSSL_MC_REQUEST;
	request.flags = RSSL_RQMF_NONE;		/* non-streaming aka snapshot */
/* No payload. */
	request.msgBase.containerType = RSSL_DT_NO_DATA;
/* Set the login token. */
	request.msgBase.streamId = token_;

/* In RFA lingo an attribute object. */
	request.msgBase.msgKey.serviceId = service_id;
	request.msgBase.msgKey.name.data   = const_cast<char*> (dictionary_name.c_str());
	request.msgBase.msgKey.name.length = static_cast<uint32_t> (dictionary_name.size());
	request.msgBase.msgKey.filter = RDM_DICTIONARY_MINIMAL;		/* for caching */
        request.msgBase.msgKey.flags = RSSL_MKF_HAS_SERVICE_ID | RSSL_MKF_HAS_NAME | RSSL_MKF_HAS_FILTER;

	buf = rsslGetBuffer (c, MAX_MSG_SIZE, RSSL_FALSE /* not packed */, &rssl_err);
	if (nullptr == buf) {
		LOG(ERROR) << prefix_ << "rsslGetBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			", \"size\": " << MAX_MSG_SIZE << ""
			", \"packedBuffer\": false"
			" }";
		return false;
	}
	rc = rsslSetEncodeIteratorBuffer (&it, buf);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorBuffer: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	rc = rsslSetEncodeIteratorRWFVersion (&it, c->majorVersion, c->minorVersion);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorRWFVersion: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"majorVersion\": " << static_cast<unsigned> (c->majorVersion) << ""
			", \"minorVersion\": " << static_cast<unsigned> (c->minorVersion) << ""
			" }";
		goto cleanup;
	}
	rc = rsslEncodeMsg (&it, reinterpret_cast<RsslMsg*> (&request));
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeMsgInit: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	buf->length = rsslGetEncodedBufferLength (&it);
	LOG_IF(WARNING, 0 == buf->length) << prefix_ << "rsslGetEncodedBufferLength returned 0.";

	DLOG(INFO) << request;
/* Message validation: must use ASSERT libraries for error description :/ */
	if (!rsslValidateMsg (reinterpret_cast<RsslMsg*> (&request))) {
		cumulative_stats_[CONSUMER_PC_MMT_DICTIONARY_MALFORMED]++;
		LOG(ERROR) << prefix_ << "rsslValidateMsg failed.";
		goto cleanup;
	} else {
		cumulative_stats_[CONSUMER_PC_MMT_DICTIONARY_VALIDATED]++;
		DVLOG(4) << prefix_ << "rsslValidateMsg succeeded.";
	}

	if (!Submit (c, buf)) {
		goto cleanup;
	}
	cumulative_stats_[CONSUMER_PC_MMT_DICTIONARY_SENT]++;
/* re-use token on failure. */
	dictionary_token_ = token_++;
	return true;
cleanup:
	cumulative_stats_[CONSUMER_PC_MMT_DICTIONARY_EXCEPTION]++;
	if (RSSL_RET_SUCCESS != rsslReleaseBuffer (buf, &rssl_err)) {
		LOG(WARNING) << prefix_ << "rsslReleaseBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
	return false;
}

bool
kinkan::consumer_t::SendItemRequest (
	RsslChannel* c,
        std::shared_ptr<item_stream_t> item_stream
	)
{
#ifndef NDEBUG
/* Static initialisation sets all fields rather than only the minimal set
 * required.  Use for debug mode and optimise for release builds.
 */
	RsslRequestMsg request = RSSL_INIT_REQUEST_MSG;
	RsslEncodeIterator it = RSSL_INIT_ENCODE_ITERATOR;
        RsslBuffer data_buffer = RSSL_INIT_BUFFER;
#else
	RsslRequestMsg request;
	RsslEncodeIterator it;
        RsslBuffer data_buffer;
	rsslClearRequestMsg (&request);
	rsslClearEncodeIterator (&it);
        rsslClearBuffer (&data_buffer);
#endif
	RsslBuffer* buf;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);
	VLOG(2) << prefix_ << "Sending MMT_MARKET_PRICE request.";

/* Set the message model type. */
	request.msgBase.domainType = RSSL_DMT_MARKET_PRICE;
/* Set request type. */
	request.msgBase.msgClass = RSSL_MC_REQUEST;
	request.flags = RSSL_RQMF_STREAMING;
/* No view thus no payload. */
	request.msgBase.containerType = RSSL_DT_NO_DATA;
/* Set the stream token. */
	request.msgBase.streamId = token_;

/* In RFA lingo an attribute object */
	request.msgBase.msgKey.nameType    = RDM_INSTRUMENT_NAME_TYPE_RIC;
	request.msgBase.msgKey.name.data   = const_cast<char*> (item_stream->item_name.c_str());
	request.msgBase.msgKey.name.length = static_cast<uint32_t> (item_stream->item_name.size());
	request.msgBase.msgKey.serviceId = service_id_;
        request.msgBase.msgKey.flags = RSSL_MKF_HAS_NAME_TYPE | RSSL_MKF_HAS_NAME | RSSL_MKF_HAS_SERVICE_ID;

	buf = rsslGetBuffer (c, MAX_MSG_SIZE, RSSL_FALSE /* not packed */, &rssl_err);
	if (nullptr == buf) {
		LOG(ERROR) << prefix_ << "rsslGetBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			", \"size\": " << MAX_MSG_SIZE << ""
			", \"packedBuffer\": false"
			" }";
		return false;
	}
	rc = rsslSetEncodeIteratorBuffer (&it, buf);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorBuffer: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	rc = rsslSetEncodeIteratorRWFVersion (&it, c->majorVersion, c->minorVersion);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslSetEncodeIteratorRWFVersion: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"majorVersion\": " << static_cast<unsigned> (c->majorVersion) << ""
			", \"minorVersion\": " << static_cast<unsigned> (c->minorVersion) << ""
			" }";
		goto cleanup;
	}
	rc = rsslEncodeMsg (&it, reinterpret_cast<RsslMsg*> (&request));
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslEncodeMsgInit: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		goto cleanup;
	}
	buf->length = rsslGetEncodedBufferLength (&it);
	LOG_IF(WARNING, 0 == buf->length) << prefix_ << "rsslGetEncodedBufferLength returned 0.";

	DLOG(INFO) << request;
/* Message validation: must use ASSERT libraries for error description :/ */
	if (!rsslValidateMsg (reinterpret_cast<RsslMsg*> (&request))) {
		cumulative_stats_[CONSUMER_PC_MMT_MARKET_PRICE_MALFORMED]++;
		LOG(ERROR) << prefix_ << "rsslValidateMsg failed.";
		goto cleanup;
	} else {
		cumulative_stats_[CONSUMER_PC_MMT_MARKET_PRICE_VALIDATED]++;
		DVLOG(4) << prefix_ << "rsslValidateMsg succeeded.";
	}

	if (!Submit (c, buf)) {
		goto cleanup;
	} else {
		cumulative_stats_[CONSUMER_PC_MMT_MARKET_PRICE_SENT]++;
/* update token state only on success, re-use token on failure. */
		auto status = tokens_.emplace (item_stream->token = token_++, item_stream);
		assert (true == status.second);
		return true;
	}
cleanup:
	cumulative_stats_[CONSUMER_PC_MMT_MARKET_PRICE_EXCEPTION]++;
	if (RSSL_RET_SUCCESS != rsslReleaseBuffer (buf, &rssl_err)) {
		LOG(WARNING) << prefix_ << "rsslReleaseBuffer: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
	return false;
}

/* Create an item stream for a given symbol name.  The Item Stream maintains
 * the provider state on behalf of the application.
 */
bool
kinkan::consumer_t::CreateItemStream (
        const char* item_name,
        std::shared_ptr<item_stream_t> item_stream
        )
{
        VLOG(4) << "Creating item stream for RIC \"" << item_name << "\".";
	item_stream->item_name.assign (item_name);
	item_stream->service_name.assign (service_name());
	if (!is_muted_) {
		if (!SendItemRequest (connection_, item_stream))
			return false;
	} else {
/* no-op */
	}
	directory_.emplace_front (item_stream);
	DVLOG(4) << "Directory size: " << directory_.size();
        last_activity_ = boost::posix_time::microsec_clock::universal_time();
	return true;
}

bool
kinkan::consumer_t::Resubscribe (
	RsslChannel* c
	)
{
	DCHECK (nullptr != c);

	if (is_muted_) {
		DVLOG(3) << "Cancelling item resubscription due to pending session.";
		return true;
	}

        std::for_each (directory_.begin(),
			directory_.end(),
			[&](std::weak_ptr<item_stream_t> it)
	{
                if (auto sp = it.lock()) {
/* only non-fulfilled items */
                        if (-1 == sp->token)
                                SendItemRequest (c, sp);
                }
        });
	return true;
}

void
kinkan::consumer_t::CreateInfo (
	kinkan::ConsumerInfo* info
	)
{
/* upstream provider name */
	info->infrastructure_address.assign ("infrastructure_address");
	info->infrastructure_version.assign ("infrastructure_version");

/* whether consumer is connected, logged in, and active */
	info->is_active = !is_muted_;

/* app level request count */
	info->msgs_received = cumulative_stats_[CONSUMER_PC_RSSL_MSGS_RECEIVED];
}

void
kinkan::consumer_t::OnActiveReadState (
	RsslChannel* c
	)
{
	RsslBuffer* buf;
	RsslReadInArgs in_args;
	RsslReadOutArgs out_args;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK (nullptr != c);

	rsslClearReadInArgs (&in_args);

	if (logging::DEBUG_MODE) {
		rsslClearReadOutArgs (&out_args);
/* In place of absent API: rsslClearError (&rssl_err); */
		rssl_err.rsslErrorId = 0;
		rssl_err.sysError = 0;
		rssl_err.text[0] = '\0';
	}

	buf = rsslReadEx (c, &in_args, &out_args, &rc, &rssl_err);
	if (logging::DEBUG_MODE) {
		std::stringstream return_code;
		if (rc > 0) {
			return_code << "\"pendingBytes\": " << static_cast<signed> (rc);
		} else {
			return_code << "\"returnCode\": \"" << static_cast<signed> (rc) << ""
				     ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\"";
		}
		VLOG(1) << "rsslReadEx: { "
			  << return_code.str() << ""
			", \"bytesRead\": " << out_args.bytesRead << ""
			", \"uncompressedBytesRead\": " << out_args.uncompressedBytesRead << ""
			", \"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}

	cumulative_stats_[CONSUMER_PC_BYTES_RECEIVED] += out_args.bytesRead;
	cumulative_stats_[CONSUMER_PC_UNCOMPRESSED_BYTES_RECEIVED] += out_args.uncompressedBytesRead;

	switch (rc) {
/* Reliable multicast events with hard-fail override. */
	case RSSL_RET_CONGESTION_DETECTED:
		cumulative_stats_[CONSUMER_PC_RSSL_CONGESTION_DETECTED]++;
		goto check_closed_state;
	case RSSL_RET_SLOW_READER:
		cumulative_stats_[CONSUMER_PC_RSSL_SLOW_READER]++;
		goto check_closed_state;
	case RSSL_RET_PACKET_GAP_DETECTED:
		cumulative_stats_[CONSUMER_PC_RSSL_PACKET_GAP_DETECTED]++;
		goto check_closed_state;
check_closed_state:
		if (RSSL_CH_STATE_CLOSED != c->state) {
			LOG(WARNING) << "rsslReadEx: { "
				  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
				", \"sysError\": " << rssl_err.sysError << ""
				", \"text\": \"" << rssl_err.text << "\""
				" }";
			break;
		}
	case RSSL_RET_READ_FD_CHANGE:
		cumulative_stats_[CONSUMER_PC_RSSL_RECONNECT]++;
		LOG(INFO) << "RSSL reconnected.";
		FD_CLR (c->oldSocketId, &in_rfds_); FD_SET (c->socketId, &in_rfds_);
		FD_CLR (c->oldSocketId, &in_efds_); FD_SET (c->socketId, &in_efds_);
		break;
	case RSSL_RET_READ_PING:
		cumulative_stats_[CONSUMER_PC_RSSL_PONG_RECEIVED]++;
		SetNextPong (last_activity_ + boost::posix_time::seconds (c->pingTimeout));
		DVLOG(1) << "RSSL pong.";
		break;
	case RSSL_RET_FAILURE:
		cumulative_stats_[CONSUMER_PC_RSSL_READ_FAILURE]++;
		LOG(ERROR) << "rsslReadEx: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
		break;
/* It is possible for rsslRead to succeed and return a NULL buffer. When this
 * occurs, it indicates that a portion of a fragmented buffer has been
 * received. The RSSL Reliable Transport is internally reassembling all parts
 * of the fragmented buffer and the entire buffer will be returned to the user
 * through rsslRead upon the arrival of the last fragment.
 */
	case RSSL_RET_SUCCESS:
	default: 
		if (nullptr != buf) {
			cumulative_stats_[CONSUMER_PC_RSSL_MSGS_RECEIVED]++;
			OnMsg (c, buf);
/* Received data equivalent to a heartbeat pong. */
			SetNextPong (last_activity_ + boost::posix_time::seconds (c->pingTimeout));
		}
		if (rc > 0) {
/* pending buffer needs flushing out before IO notification can resume */
			FD_SET (c->socketId, &out_rfds_);
		}
		break;
	}
}

void
kinkan::consumer_t::OnMsg (
	RsslChannel* handle,
	RsslBuffer* buf		/* nullptr indicates a partially received fragmented message and thus invalid for processing */
	)
{
#ifndef NDEBUG
	RsslDecodeIterator it = RSSL_INIT_DECODE_ITERATOR;
	RsslMsg msg = RSSL_INIT_MSG;
#else
	RsslDecodeIterator it;
	RsslMsg msg;
	rsslClearDecodeIterator (&it);
	rsslClearMsg (&msg);
#endif
	RsslRet rc;

	DCHECK(handle != nullptr);
	DCHECK(buf != nullptr);

/* Prepare codec */
	rc = rsslSetDecodeIteratorRWFVersion (&it, handle->majorVersion, handle->minorVersion);
	if (RSSL_RET_SUCCESS != rc) {
/* Unsupported version or internal error, close out the connection. */
		cumulative_stats_[CONSUMER_PC_RWF_VERSION_UNSUPPORTED]++;
		Abort (handle);
		LOG(ERROR) << "rsslSetDecodeIteratorRWFVersion: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			", \"majorVersion\": " << static_cast<unsigned> (handle->majorVersion) << ""
			", \"minorVersion\": " << static_cast<unsigned> (handle->minorVersion) << ""
			" }";
		return;
	}
	rc = rsslSetDecodeIteratorBuffer (&it, buf);
	if (RSSL_RET_SUCCESS != rc) {
/* Invalid buffer or internal error, discard the message. */
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED]++;
		Abort (handle);
		LOG(ERROR) << "rsslSetDecodeIteratorBuffer: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		return;
	}

/* Decode data buffer into RSSL message */
	rc = rsslDecodeMsg (&it, &msg);
	if (RSSL_RET_SUCCESS != rc) {
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED]++;
		Abort (handle);
		LOG(WARNING) << "rsslDecodeMsg: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		return;
	} else {
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_DECODED]++;
		if (logging::DEBUG_MODE) {
/* Pass through RSSL validation and report exceptions */
			if (!rsslValidateMsg (&msg)) {
				cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED]++;
				LOG(WARNING) << "rsslValidateMsg failed.";
				Abort (handle);
				return;
			} else {
				cumulative_stats_[CONSUMER_PC_RSSL_MSGS_VALIDATED]++;
				DVLOG(4) << "rsslValidateMsg success.";
			}
			DVLOG(3) << msg;
		}
		if (!OnMsg (handle, &it, &msg))
			Abort (handle);
	}
}

/* Returns true if message processed successfully, returns false to abort the connection.
 */
bool
kinkan::consumer_t::OnMsg (
	RsslChannel* c,
	RsslDecodeIterator* it,
	RsslMsg* msg
	)
{
        DCHECK (nullptr != it);
        DCHECK (nullptr != msg);
        cumulative_stats_[CONSUMER_PC_RSSL_MSGS_RECEIVED]++;

        switch (msg->msgBase.domainType) {
	case RSSL_DMT_LOGIN:
                return OnLoginResponse (c, it, msg);
	case RSSL_DMT_MARKET_PRICE:
		return OnMarketPrice (c, it, msg);
	case RSSL_DMT_SOURCE:
		return OnDirectory (c, it, msg);
	case RSSL_DMT_DICTIONARY:
		return OnDictionary (c, it, msg);
	default:
                cumulative_stats_[CONSUMER_PC_RSSL_MSGS_REJECTED]++;
                LOG(WARNING) << prefix_ << "Uncaught message: " << msg;
                return true;
        }
}

bool
kinkan::consumer_t::OnLoginResponse (
	RsslChannel* c,
	RsslDecodeIterator* it,
	RsslMsg* msg
	)
{
	RsslRDMLoginMsg response;
	char buffer[1024];
	RsslBuffer data_buffer = { static_cast<uint32_t> (sizeof (buffer)), buffer };
	RsslState state = RSSL_INIT_STATE;
	RsslErrorInfo rssl_err_info;
	RsslRet rc;

        DCHECK (nullptr != it);
        DCHECK (nullptr != msg);

        cumulative_stats_[CONSUMER_PC_MMT_LOGIN_RECEIVED]++;
	rc = rsslDecodeRDMLoginMsg (it, msg, &response, &data_buffer, &rssl_err_info);
	if (RSSL_RET_BUFFER_TOO_SMALL == rc) {
		LOG(ERROR) << prefix_ << "rsslDecodeRDMLoginMsg: { "
				  "\"returnCode\": " << static_cast<signed> (rc) << ""
				", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
				", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
				" }";
		return true;
	}
	if (RSSL_RET_SUCCESS != rc) {
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED]++;
		LOG(WARNING) << prefix_ << "rsslDecodeRDMLoginMsg: { "
				  "\"rsslErrorInfoCode\": \"" << internal::error_info_code_string (rssl_err_info.rsslErrorInfoCode) << "\""
				", \"rsslError\": { "
					  "\"rsslErrorId\": " << rssl_err_info.rsslError.rsslErrorId << ""
					", \"sysError\": " << rssl_err_info.rsslError.sysError << ""
					", \"text\": \"" << rssl_err_info.rsslError.text << "\""
					" }"
				", \"errorLocation\": \"" << rssl_err_info.errorLocation << "\""
				" }";
		return true;
	}

/* extract out stream and data state like RFA */
	switch (response.rdmMsgBase.rdmMsgType) {
	case RDM_LG_MT_REFRESH:
		state.streamState = response.refresh.state.streamState;
		state.dataState = response.refresh.state.dataState;
		break;

	case RDM_LG_MT_CLOSE:
		state.streamState = RSSL_STREAM_CLOSED;
		break;

	case RDM_LG_MT_STATUS:
		state.streamState = response.status.state.streamState;
		state.dataState = response.status.state.dataState;
		break;
		
	case RDM_LG_MT_UNKNOWN:
	case RDM_LG_MT_REQUEST:
	case RDM_LG_MT_CONSUMER_CONNECTION_STATUS:
	case RDM_LG_MT_POST:
	case RDM_LG_MT_ACK:
	default:
                cumulative_stats_[CONSUMER_PC_MMT_LOGIN_DISCARDED]++;
		LOG(WARNING) << prefix_ << "Uncaught: " << msg;
	}

	switch (state.streamState) {
	case RSSL_STREAM_OPEN:
		switch (state.dataState) {
		case RSSL_DATA_OK:
			return OnLoginSuccess (c, response);
		case RSSL_DATA_SUSPECT:
			return OnLoginSuspect (c, response);
		case RSSL_DATA_NO_CHANGE:
// by-definition, ignore
			return true;;
		default:
			LOG(WARNING) << prefix_ << "Uncaught data state: " << msg;
			return true;
		}

	case RSSL_STREAM_CLOSED:
		return OnLoginClosed (c, response);

	default:
		LOG(WARNING) << prefix_ << "Uncaught stream state: " << msg;
		return true;
	}
}

bool
kinkan::consumer_t::OnDirectory (
	RsslChannel* c,
	RsslDecodeIterator* it,
	RsslMsg* msg
	)
{
	RsslRDMDirectoryMsg response;
	char buffer[16384];
	RsslBuffer data_buffer = { static_cast<uint32_t> (sizeof (buffer)), buffer };
	RsslErrorInfo rssl_err_info;
	RsslRet rc;

        DCHECK (nullptr != it);
        DCHECK (nullptr != msg);

	DLOG(INFO) << "OnDirectory";

        cumulative_stats_[CONSUMER_PC_MMT_DIRECTORY_RECEIVED]++;
	rc = rsslDecodeRDMDirectoryMsg (it, msg, &response, &data_buffer, &rssl_err_info);
	if (RSSL_RET_BUFFER_TOO_SMALL == rc) {
		LOG(ERROR) << prefix_ << "rsslDecodeRDMDirectoryMsg: { "
				  "\"returnCode\": " << static_cast<signed> (rc) << ""
				", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
				", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
				" }";
		return true;
	}
	if (RSSL_RET_SUCCESS != rc) {
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED]++;
		LOG(WARNING) << prefix_ << "rsslDecodeRDMDirectoryMsg: { "
				  "\"rsslErrorInfoCode\": \"" << internal::error_info_code_string (rssl_err_info.rsslErrorInfoCode) << "\""
				", \"rsslError\": { "
					  "\"rsslErrorId\": " << rssl_err_info.rsslError.rsslErrorId << ""
					", \"sysError\": " << rssl_err_info.rsslError.sysError << ""
					", \"text\": \"" << rssl_err_info.rsslError.text << "\""
					" }"
				", \"errorLocation\": \"" << rssl_err_info.errorLocation << "\""
				" }";
		return true;
	}

	switch (response.rdmMsgBase.rdmMsgType) {
	case RDM_DR_MT_REFRESH:
		return OnDirectoryRefresh (c, response.refresh);
	case RDM_DR_MT_UPDATE:
		return OnDirectoryUpdate (c, response.update);

	case RDM_DR_MT_CLOSE:
	case RDM_DR_MT_STATUS:
	default:
		LOG(WARNING) << prefix_ << "Uncaught directory response message type: " << msg;
		return true;
	}
}

/* Connecting to TREP-RT infrastructure, i.e. an ADS then an RDM dictionary can be 
 * requested from any listed service and will be the dictionary ADS loads from its
 * local configuration.
 */
bool
kinkan::consumer_t::OnDirectoryRefresh (
	RsslChannel* c,
	const RsslRDMDirectoryRefresh& response
	)
{
        DCHECK (nullptr != c);

	DLOG(INFO) << "OnDirectoryRefresh";
	for (uint_fast32_t i = 0; i < response.serviceCount; ++i)
	{
		const RsslRDMService& service = response.serviceList[i];
		const std::string service_name (service.info.serviceName.data, service.info.serviceName.length);
		if (0 == service_name.compare (this->service_name())) {
			SetServiceId (static_cast<uint16_t> (service.serviceId));
			break;
		}
	}

/* Request on first directory message, can be messy with multiple refresh messages
 * being received before dictionary response.
 */
	if (!rdm_dictionary_.isInitialized) {
		if (0 == response.serviceCount) {
			LOG(WARNING) << prefix_ << "Upstream provider has no configured services, unable to request a dictionary.";
			return true;
		}
		const RsslRDMService& service = response.serviceList[0];
/* Hard code to RDM dictionary for TREP deployment. */
		if (!SendDictionaryRequest (c, static_cast<uint16_t> (service.serviceId), kRdmFieldDictionaryName))
			return false;
	}

	return Resubscribe (c);
}

bool
kinkan::consumer_t::OnDirectoryUpdate (
	RsslChannel* c,
	const RsslRDMDirectoryUpdate& response
	)
{
        DCHECK (nullptr != c);

	DLOG(INFO) << "OnDirectoryUpdate";
	for (uint_fast32_t i = 0; i < response.serviceCount; ++i)
	{
		const RsslRDMService& service = response.serviceList[i];
		const std::string service_name (service.info.serviceName.data, service.info.serviceName.length);
		if (0 == service_name.compare (this->service_name())) {
			SetServiceId (static_cast<uint16_t> (service.serviceId));
			break;
		}
	}
	return Resubscribe (c);
}

bool
kinkan::consumer_t::OnDictionary (
	RsslChannel* c,
	RsslDecodeIterator* it,
	RsslMsg* msg
	)
{
	RsslRDMDictionaryMsg response;
	char buffer[8192];
	RsslBuffer data_buffer = { static_cast<uint32_t> (sizeof (buffer)), buffer };
	RsslErrorInfo rssl_err_info;
	RsslRet rc;

        DCHECK (nullptr != it);
        DCHECK (nullptr != msg);

	DLOG(INFO) << "OnDictionary";

        cumulative_stats_[CONSUMER_PC_MMT_DICTIONARY_RECEIVED]++;
	rc = rsslDecodeRDMDictionaryMsg (it, msg, &response, &data_buffer, &rssl_err_info);
	if (RSSL_RET_BUFFER_TOO_SMALL == rc) {
		LOG(ERROR) << prefix_ << "rsslDecodeRDMDictionaryMsg: { "
				  "\"returnCode\": " << static_cast<signed> (rc) << ""
				", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
				", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
				" }";
		return true;
	}
	if (RSSL_RET_SUCCESS != rc) {
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_MALFORMED]++;
		LOG(WARNING) << prefix_ << "rsslDecodeRDMDirectoryMsg: { "
				  "\"rsslErrorInfoCode\": \"" << internal::error_info_code_string (rssl_err_info.rsslErrorInfoCode) << "\""
				", \"rsslError\": { "
					  "\"rsslErrorId\": " << rssl_err_info.rsslError.rsslErrorId << ""
					", \"sysError\": " << rssl_err_info.rsslError.sysError << ""
					", \"text\": \"" << rssl_err_info.rsslError.text << "\""
					" }"
				", \"errorLocation\": \"" << rssl_err_info.errorLocation << "\""
				" }";
		return true;
	}

	switch (response.rdmMsgBase.rdmMsgType) {
	case RDM_DC_MT_REFRESH:
		return OnDictionaryRefresh (c, it, response.refresh);
/* Status can show a new dictionary but is not implemented in TREP-RT infrastructure, so ignore. */
	case RDM_DC_MT_STATUS:
/* Close should only happen when the infrastructure is in shutdown, defer to closed MMT_LOGIN. */
	case RDM_DC_MT_CLOSE:
	default:
		LOG(WARNING) << prefix_ << "Uncaught dictionay response message type: " << msg;
		return true;
	}
}

/* Replace any existing RDM dictionary upon a dictionary refresh message.
 */

bool
kinkan::consumer_t::OnDictionaryRefresh (
	RsslChannel* c,
	RsslDecodeIterator* it,
	const RsslRDMDictionaryRefresh& response
	)
{
	char buffer[1024];
	RsslBuffer data_buffer = { static_cast<uint32_t> (sizeof (buffer)), buffer };
	RsslRet rc;

        DCHECK (nullptr != c);

	DLOG(INFO) << "OnDictionaryRefresh";
	rc = rsslDecodeFieldDictionary (it, &rdm_dictionary_, RDM_DICTIONARY_MINIMAL, &data_buffer);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(ERROR) << prefix_ << "rsslDecodeFieldDictionary: { "
			  "\"text\": \"" << std::string (data_buffer.data, data_buffer.length) << "\""
			" }";
		return false;
	}

/* Multiple calls on the same instance may receive the following:
 * {
 *   "rsslErrorId": -1,
 *   "text": "Dictionary reload failure: Field ID 2 definition not matching in current and new dictionary"
 * }
 */
	if (0 != (response.flags & RDM_DC_RFF_IS_COMPLETE)) {
		RsslPayloadCacheConfigOptions cache_config;
		RsslCacheError rssl_cache_err;

		rsslCacheErrorClear (&rssl_cache_err);

		VLOG(3) << "Dictionary reception complete.";

/* Re/build cache on demand to permit new dictionary. */
		if (nullptr != cache_handle_) {
			rsslPayloadCacheDestroy (cache_handle_);
		}
		cache_config.maxItems = 0;	// unlimited
		cache_handle_ = rsslPayloadCacheCreate (&cache_config, &rssl_cache_err);
		if (nullptr == cache_handle_) {
			LOG(ERROR) << prefix_ << "rsslPayloadCacheCreate: { "
				  "\"rsslErrorId\": " << rssl_cache_err.rsslErrorId << ""
				", \"text\": \"" << rssl_cache_err.text << "\""
				" }";
			return false;
		}
/* Bind the new dictionary into the cache object. */
		rc = rsslPayloadCacheSetDictionary (cache_handle_, &rdm_dictionary_, kRdmFieldDictionaryName.c_str(), &rssl_cache_err);
		if (RSSL_RET_SUCCESS != rc) {
			LOG(ERROR) << prefix_ << "rsslPayloadCacheSetDictionary: { "
				  "\"rsslErrorId\": " << rssl_cache_err.rsslErrorId << ""
				", \"text\": \"" << rssl_cache_err.text << "\""
				" }";
			return false;
		}
/* Permit new subscriptions. */
		is_muted_ = false;
		return Resubscribe (c);
	}
	return true;
}

bool
kinkan::consumer_t::OnLoginSuccess (
	RsslChannel* c,
	const RsslRDMLoginMsg& response
	)
{
	DLOG(INFO) << "OnLoginSuccess";
/* Log upstream application name, only presented in refresh messages. */
	switch (response.rdmMsgBase.rdmMsgType) {
	case RDM_LG_MT_REFRESH:
		if (0 != (response.refresh.flags & RDM_LG_RFF_HAS_APPLICATION_NAME)) {
			chromium::StringPiece application_name (response.refresh.applicationName.data,
								response.refresh.applicationName.length);
			LOG(INFO) << prefix_ << "applicationName: \"" << application_name << "\"";
		}
	default:
		break;
	}
/* A new connection to TREP infrastructure, request dictionary to discover available services. */
	return SendDirectoryRequest (c);
}

bool
kinkan::consumer_t::OnLoginSuspect (
	RsslChannel* c,
	const RsslRDMLoginMsg& response
	)
{
	DLOG(INFO) << "OnLoginSuspect";
	is_muted_ = true;
	return true;
}

bool
kinkan::consumer_t::OnLoginClosed (
	RsslChannel* c,
	const RsslRDMLoginMsg& response
	)
{
	DLOG(INFO) << "OnLoginClosed";
	is_muted_ = true;
	return true;
}

bool
kinkan::consumer_t::OnMarketPrice (
	RsslChannel* handle,
	RsslDecodeIterator* it,
	RsslMsg* msg
	)
{
	bool rc = true;

	DCHECK(nullptr != handle);
        DCHECK(nullptr != it);
        DCHECK(nullptr != msg);
	DCHECK(!tokens_.empty());
	DCHECK(tokens_.find (msg->msgBase.streamId) != tokens_.end());

        cumulative_stats_[CONSUMER_PC_MMT_MARKET_PRICE_RECEIVED]++;
/* complete failure if we are sent a bogus token */
	auto stream = tokens_[msg->msgBase.streamId].lock();

/* Verify stream state. */
	if (rsslIsFinalMsg (msg)) {
		VLOG(2) << "Stream closed for \"" << stream->item_name << "\".";
		if (!stream->is_closed) {
			refresh_count_++;
			stream->is_closed = true;
		}
	}

	switch(msg->msgBase.msgClass) {
        case RSSL_MC_REFRESH:
		if (0 == stream->refresh_received++) {
			refresh_count_++;
		}
		rc = OnMarketPriceRefresh (handle, it, msg, stream);
		break;

        case RSSL_MC_UPDATE:
		stream->update_received++;
		return OnMarketPriceUpdate (handle, it, msg, stream);

        case RSSL_MC_STATUS:
		VLOG(1) << "Ignoring status";
		stream->status_received++;
		break;

        case RSSL_MC_CLOSE:
        case RSSL_MC_ACK:
        case RSSL_MC_GENERIC:
        default:
                cumulative_stats_[CONSUMER_PC_RSSL_MSGS_REJECTED]++;
                LOG(WARNING) << prefix_ << "Uncaught message: " << msg;
                return rc;
        }

/* Refresh state check */
	if (!in_sync_ && refresh_count_ == directory_.size()) {
		in_sync_ = true;
		LOG(INFO) << "Service " << stream->service_name << " synchronized.";
		delegate_->OnSync();
	}

	return rc;
}

bool
kinkan::consumer_t::OnMarketPriceRefresh (
	RsslChannel* handle,
	RsslDecodeIterator* it,
	RsslMsg* msg,
	std::shared_ptr<item_stream_t> stream
	)
{
	return OnMarketPriceUpdate (handle, it, msg, stream);
}

bool
kinkan::consumer_t::OnMarketPriceUpdate (
	RsslChannel* handle,
	RsslDecodeIterator* it,
	RsslMsg* msg,
	std::shared_ptr<item_stream_t> stream
	)
{
	RsslCacheError rssl_cache_err;
	RsslRet rc;

	DCHECK(nullptr != handle);
	DCHECK(nullptr != it);
	DCHECK(nullptr != msg);
	DCHECK(nullptr != cache_handle_);

	rsslCacheErrorClear (&rssl_cache_err);

/* lazy cache creation */
	if (nullptr == stream->payload_entry_handle) {
		DVLOG(3) << "Creating payload entry for \"" << stream->item_name << "\"";
		stream->payload_entry_handle = rsslPayloadEntryCreate (cache_handle_, &rssl_cache_err);
		if (nullptr == stream->payload_entry_handle) {
			LOG(ERROR) << prefix_ << "rsslPayloadEntryCreate: { "
				  "\"rsslErrorId\": " << rssl_cache_err.rsslErrorId << ""
				", \"text\": \"" << rssl_cache_err.text << "\""
				" }";
			return false;
		}
	}

/* Copy-on-write */
	if (!delegate_->OnWrite (stream, handle->majorVersion, handle->minorVersion, msg))
		return false;

	rc = rsslSetDecodeIteratorBuffer (it, &msg->msgBase.encDataBody);
	if (RSSL_RET_SUCCESS != rc) {
/* Invalid buffer or internal error, discard the message. */
		LOG(ERROR) << prefix_ << "rsslSetDecodeIteratorBuffer: { "
			  "\"returnCode\": " << static_cast<signed> (rc) << ""
			", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\""
			", \"text\": \"" << rsslRetCodeInfo (rc) << "\""
			" }";
		return false;
	}
	DVLOG(3) << "Applying payload update for \"" << stream->item_name << "\"";
	rc = rsslPayloadEntryApply (stream->payload_entry_handle, it, msg, &rssl_cache_err);
	if (RSSL_RET_SUCCESS != rc) {
		LOG(WARNING) << prefix_ << "rsslPayloadEntryApply: { "
				  "\"rsslErrorId\": " << rssl_cache_err.rsslErrorId << ""
				", \"text\": \"" << rssl_cache_err.text << "\""
				" }";
		return false;
	}
	return true;
}

int
kinkan::consumer_t::Submit (
	RsslChannel* c,
	RsslBuffer* buf
	)
{
	RsslWriteInArgs in_args;
	RsslWriteOutArgs out_args;
	RsslError rssl_err;
	RsslRet rc;

	DCHECK(nullptr != c);
	DCHECK(nullptr != buf);

	rsslClearWriteInArgs (&in_args);
	in_args.rsslPriority = RSSL_LOW_PRIORITY;	/* flushing priority */
/* direct write on clear socket, enqueue when writes are pending */
	const bool should_write_direct = !FD_ISSET (c->socketId, &in_wfds_);
	in_args.writeInFlags = should_write_direct ? RSSL_WRITE_DIRECT_SOCKET_WRITE : 0;

try_again:
	if (logging::DEBUG_MODE) {
		rsslClearWriteOutArgs (&out_args);
/* rsslClearError (&rssl_err); */
		rssl_err.rsslErrorId = 0;
		rssl_err.sysError = 0;
		rssl_err.text[0] = '\0';
	}
	rc = rsslWriteEx (c, buf, &in_args, &out_args, &rssl_err);
	if (logging::DEBUG_MODE) {
		std::stringstream return_code;
		if (rc > 0) {
			return_code << "\"pendingBytes\": " << rc;
		} else {
			return_code << "\"returnCode\": \"" << static_cast<signed> (rc) << ""
				     ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\"";
		}
		VLOG(1) << "rsslWriteEx: { "
			  << return_code.str() << ""
			", \"bytesWritten\": " << out_args.bytesWritten << ""
			", \"uncompressedBytesWritten\": " << out_args.uncompressedBytesWritten << ""
			", \"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
	if (rc > 0) {
		IncrementPendingCount();
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_ENQUEUED]++;
		goto pending;
	}
	switch (rc) {
	case RSSL_RET_WRITE_CALL_AGAIN:			/* fragmenting the buffer and needs to be called again with the same buffer. */
		goto try_again;
	case RSSL_RET_WRITE_FLUSH_FAILED:		/* attempted to flush data to the connection but was blocked. */
		cumulative_stats_[CONSUMER_PC_RSSL_WRITE_FLUSH_FAILED]++;
		goto pending;
	case RSSL_RET_BUFFER_NO_BUFFERS:		/* empty buffer pool: spin wait until buffer is available. */
		cumulative_stats_[CONSUMER_PC_RSSL_WRITE_NO_BUFFERS]++;
pending:
		FD_SET (c->socketId, &in_wfds_);	/* pending output */
		return -1;
	case RSSL_RET_SUCCESS:				/* sent, no flush required. */
		cumulative_stats_[CONSUMER_PC_RSSL_MSGS_SENT]++;
/* Sent data equivalent to a ping. */
		SetNextPing (last_activity_ + boost::posix_time::seconds (ping_interval_));
		return 1;
	default:
		cumulative_stats_[CONSUMER_PC_RSSL_WRITE_EXCEPTION]++;
		LOG(ERROR) << "rsslWriteEx: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
		return 0;
	}
}

int
kinkan::consumer_t::Ping (
	RsslChannel* c
	)
{
	RsslError rssl_err;
	RsslRet rc;

	DCHECK(nullptr != c);

	if (logging::DEBUG_MODE) {
/* In place of absent API: rsslClearError (&rssl_err); */
		rssl_err.rsslErrorId = 0;
		rssl_err.sysError = 0;
		rssl_err.text[0] = '\0';
	}
	rc = rsslPing (c, &rssl_err);
	if (logging::DEBUG_MODE && VLOG_IS_ON(1)) {
		std::stringstream return_code;
		if (rc > 0) {
			return_code << "\"pendingBytes\": " << static_cast<signed> (rc);
		} else {
			return_code << "\"returnCode\": \"" << static_cast<signed> (rc) << ""
				     ", \"enumeration\": \"" << rsslRetCodeToString (rc) << "\"";
		}
		VLOG(1) << "rsslPing: { "
			  << return_code.str() << ""
			", \"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
	}
	if (rc > 0) goto pending;
	switch (rc) {
	case RSSL_RET_WRITE_FLUSH_FAILED:		/* attempted to flush data to the connection but was blocked. */
		cumulative_stats_[CONSUMER_PC_RSSL_PING_FLUSH_FAILED]++;
		goto pending;
	case RSSL_RET_BUFFER_NO_BUFFERS:		/* empty buffer pool: spin wait until buffer is available. */
		cumulative_stats_[CONSUMER_PC_RSSL_PING_NO_BUFFERS]++;
pending:
/* Pings should only occur when no writes are pending, thus rsslPing internally calls rsslFlush
 * automatically.  If this fails then either the client has stalled or the systems is out of 
 * resources.  Suitable consequence is to force a disconnect.
 */
		FD_SET (c->socketId, &out_efds_);
		LOG(INFO) << "rsslPing: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
		return -1;
	case RSSL_RET_SUCCESS:				/* sent, no flush required. */
		cumulative_stats_[CONSUMER_PC_RSSL_PING_SENT]++;
/* Advance ping expiration only on success. */
		SetNextPing (last_activity_ + boost::posix_time::seconds (ping_interval_));
		return 1;
	default:
		cumulative_stats_[CONSUMER_PC_RSSL_PING_EXCEPTION]++;
		LOG(ERROR) << "rsslPing: { "
			  "\"rsslErrorId\": " << rssl_err.rsslErrorId << ""
			", \"sysError\": " << rssl_err.sysError << ""
			", \"text\": \"" << rssl_err.text << "\""
			" }";
		return 0;
	}
}

/* eof */
