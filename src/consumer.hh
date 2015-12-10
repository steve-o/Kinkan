/* UPA consumer.
 */

#ifndef CONSUMER_HH_
#define CONSUMER_HH_

#include <winsock2.h>

#include <cstdint>
#include <list>
#include <memory>
#include <boost/unordered_map.hpp>
#include <unordered_set>
#include <utility>

/* Boost Atomics */
#include <boost/atomic.hpp>

/* Boost Posix Time */
#include <boost/date_time/posix_time/posix_time.hpp>

/* Boost threading. */
#include <boost/thread.hpp>

/* UPA 8.0 */
#include <upa/upa.h>
#include <rtr/rsslRDMLoginMsg.h>
#include <rtr/rsslRDMDictionaryMsg.h>
#include <rtr/rsslRDMDirectoryMsg.h>
#include <rtr/rsslPayloadCache.h>

#include "chromium/debug/leak_tracker.hh"
#include "chromium/message_loop/message_pump.hh"
#include "chromium/strings/string_piece.hh"
#include "net/socket/socket_descriptor.hh"
#include "upa.hh"
#include "client.hh"
#include "config.hh"
#include "deleter.hh"
#include "kinkan_http_server.hh"
#include "message_loop.hh"

namespace kinkan
{
/* Performance Counters */
	enum {
		CONSUMER_PC_BYTES_RECEIVED,
		CONSUMER_PC_UNCOMPRESSED_BYTES_RECEIVED,
		CONSUMER_PC_MSGS_SENT,
		CONSUMER_PC_RSSL_MSGS_ENQUEUED,
		CONSUMER_PC_RSSL_MSGS_SENT,
		CONSUMER_PC_RSSL_MSGS_RECEIVED,
                CONSUMER_PC_RSSL_MSGS_REJECTED,
		CONSUMER_PC_RSSL_MSGS_DECODED,
		CONSUMER_PC_RSSL_MSGS_MALFORMED,
		CONSUMER_PC_RSSL_MSGS_VALIDATED,
		CONSUMER_PC_CONNECTION_INITIATED,
		CONSUMER_PC_CONNECTION_REJECTED,
		CONSUMER_PC_CONNECTION_ACCEPTED,
		CONSUMER_PC_CONNECTION_EXCEPTION,
		CONSUMER_PC_RWF_VERSION_UNSUPPORTED,
		CONSUMER_PC_RSSL_PING_SENT,
		CONSUMER_PC_RSSL_PONG_RECEIVED,
		CONSUMER_PC_RSSL_PONG_TIMEOUT,
		CONSUMER_PC_RSSL_PROTOCOL_DOWNGRADE,
		CONSUMER_PC_RSSL_FLUSH,
		CONSUMER_PC_OMM_ACTIVE_CLIENT_SESSION_RECEIVED,
		CONSUMER_PC_OMM_ACTIVE_CLIENT_SESSION_EXCEPTION,
		CONSUMER_PC_CLIENT_SESSION_REJECTED,
		CONSUMER_PC_CLIENT_SESSION_ACCEPTED,
		CONSUMER_PC_RSSL_RECONNECT,
		CONSUMER_PC_RSSL_CONGESTION_DETECTED,
		CONSUMER_PC_RSSL_SLOW_READER,
		CONSUMER_PC_RSSL_PACKET_GAP_DETECTED,
		CONSUMER_PC_RSSL_READ_FAILURE,
		CONSUMER_PC_CLIENT_INIT_EXCEPTION,
		CONSUMER_PC_DIRECTORY_MAP_EXCEPTION,
		CONSUMER_PC_RSSL_PING_EXCEPTION,
		CONSUMER_PC_RSSL_PING_FLUSH_FAILED,
		CONSUMER_PC_RSSL_PING_NO_BUFFERS,
		CONSUMER_PC_RSSL_WRITE_EXCEPTION,
		CONSUMER_PC_RSSL_WRITE_FLUSH_FAILED,
		CONSUMER_PC_RSSL_WRITE_NO_BUFFERS,
                CONSUMER_PC_RESPONSE_MSGS_RECEIVED,
                CONSUMER_PC_RESPONSE_MSGS_DISCARDED,
                CONSUMER_PC_MMT_LOGIN_RECEIVED,
                CONSUMER_PC_MMT_LOGIN_DISCARDED,
                CONSUMER_PC_MMT_LOGIN_SUCCESS,
                CONSUMER_PC_MMT_LOGIN_SUSPECT,
                CONSUMER_PC_MMT_LOGIN_CLOSED,
                CONSUMER_PC_MMT_LOGIN_VALIDATED,
                CONSUMER_PC_MMT_LOGIN_MALFORMED,
                CONSUMER_PC_MMT_LOGIN_EXCEPTION,
                CONSUMER_PC_MMT_LOGIN_SENT,
                CONSUMER_PC_MMT_DIRECTORY_MALFORMED,
                CONSUMER_PC_MMT_DIRECTORY_VALIDATED,
                CONSUMER_PC_MMT_DIRECTORY_RECEIVED,
                CONSUMER_PC_MMT_DIRECTORY_EXCEPTION,
                CONSUMER_PC_MMT_DIRECTORY_SENT,
                CONSUMER_PC_MMT_DICTIONARY_MALFORMED,
                CONSUMER_PC_MMT_DICTIONARY_VALIDATED,
                CONSUMER_PC_MMT_DICTIONARY_RECEIVED,
                CONSUMER_PC_MMT_DICTIONARY_EXCEPTION,
                CONSUMER_PC_MMT_DICTIONARY_SENT,
                CONSUMER_PC_MMT_MARKET_PRICE_RECEIVED,
                CONSUMER_PC_MMT_MARKET_PRICE_VALIDATED,
                CONSUMER_PC_MMT_MARKET_PRICE_MALFORMED,
                CONSUMER_PC_MMT_MARKET_PRICE_EXCEPTION,
                CONSUMER_PC_MMT_MARKET_PRICE_SENT,
/* marker */
		CONSUMER_PC_MAX
	};

	class item_stream_t
	{
	public:
		explicit item_stream_t()
			: token (-1),
			  payload_entry_handle (nullptr),
			  msg_count (0),
			  last_activity (boost::posix_time::second_clock::universal_time()),
			  refresh_received (0),
			  status_received (0),
			  update_received (0),
			  is_closed (false)
		{
		}

/* Fixed name for this stream. */
		std::string item_name;

/* Service origin, e.g. IDN_RDF */
		std::string service_name;

/* Subscription handle which is valid from login success to login close. */
		int32_t token;

/* Last value cache, created on-demand. */
		RsslPayloadEntryHandle payload_entry_handle;

/* Performance counters */
		boost::posix_time::ptime last_activity;
		boost::posix_time::ptime last_refresh;
		boost::posix_time::ptime last_status;
		boost::posix_time::ptime last_update;
		uint32_t msg_count;		/* including unknown message types */
		uint32_t refresh_received;
		uint32_t status_received;
		uint32_t update_received;

		bool is_closed;
	};

	class consumer_t
		: public std::enable_shared_from_this<consumer_t>
		, public chromium::MessageLoop
		, public chromium::MessagePump
		, public KinkanHttpServer::ConsumerDelegate
	{
	public:
/* Delegate handles specific behaviour of service subscription status. */
		class Delegate {
                public:
                    Delegate() {}
                
                    virtual bool OnSync() = 0;
		    virtual bool OnTrigger() = 0;
                    virtual bool OnWrite (std::shared_ptr<item_stream_t> item_stream, const uint8_t rwf_major_version, const uint8_t rwf_minor_version, RsslMsg* msg) = 0;
                
                protected:
                    virtual ~Delegate() {}
                };

		explicit consumer_t (const config_t& config, std::shared_ptr<upa_t> upa, Delegate* delegate);
		~consumer_t();

		bool Initialize();
		void Close();

// MessagePump methods:
		virtual void Run() override;
		virtual void Quit() override;
		virtual void ScheduleWork() override;
		virtual void ScheduleDelayedWork(const std::chrono::steady_clock::time_point& delayed_work_time) override;
		void OnWakeup();

		bool CreateItemStream (const char* name, std::shared_ptr<item_stream_t> item_stream);
		bool Resubscribe (RsslChannel* handle);

// ConsumerDelegate methods:
		virtual void CreateInfo(ConsumerInfo* info) override;

		static uint8_t rwf_major_version (uint16_t rwf_version) { return rwf_version / 256; }
		static uint8_t rwf_minor_version (uint16_t rwf_version) { return rwf_version % 256; }
		const char* service_name() const {
			return config_.upstream_service_name.c_str();
		}
		uint16_t service_id() const {
			return service_id_;
		}
/* temporary */
		RsslPayloadCacheHandle cache_handle() {
			return cache_handle_;
		}

	private:
		bool DoInternalWork();

		void Connect();

		void OnCanReadWithoutBlocking (RsslChannel* handle);
		void OnCanWriteWithoutBlocking (RsslChannel* handle);
		void Abort (RsslChannel* handle);
		void Close (RsslChannel* handle);

		void OnInitializingState (RsslChannel* handle);
		bool OnActiveSession (RsslChannel* handle);

		void OnActiveReadState (RsslChannel* handle);
		void OnActiveWriteState (RsslChannel* handle);
		void OnMsg (RsslChannel* handle, RsslBuffer* buf);
		bool OnMsg (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg);
		bool OnLoginResponse (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg);
		bool OnLoginSuccess (RsslChannel* c, const RsslRDMLoginMsg& response);
		bool OnLoginSuspect (RsslChannel* c, const RsslRDMLoginMsg& response);
		bool OnLoginClosed (RsslChannel* c, const RsslRDMLoginMsg& response);
		bool OnDirectory (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg);
		bool OnDirectoryRefresh (RsslChannel* c, const RsslRDMDirectoryRefresh& response);
		bool OnDirectoryUpdate (RsslChannel* c, const RsslRDMDirectoryUpdate& response);
		bool OnDictionary (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg);
		bool OnDictionaryRefresh (RsslChannel* c, RsslDecodeIterator* it, const RsslRDMDictionaryRefresh& response);
		bool OnMarketPrice (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg);
		bool OnMarketPriceRefresh (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg, std::shared_ptr<item_stream_t> stream);
		bool OnMarketPriceUpdate (RsslChannel* c, RsslDecodeIterator* it, RsslMsg* msg, std::shared_ptr<item_stream_t> stream);

		bool SendLoginRequest (RsslChannel* c);
		bool SendDirectoryRequest (RsslChannel* c);
		bool SendDictionaryRequest (RsslChannel* c, const uint16_t service_id, const std::string& dictionary_name);
		bool SendItemRequest (RsslChannel* c, std::shared_ptr<item_stream_t> item_stream);

		int Submit (RsslChannel* c, RsslBuffer* buf);
		int Ping (RsslChannel* c);

		void SetServiceId (uint16_t service_id) {
			service_id_.store (service_id);
		}
                const boost::posix_time::ptime& NextPing() const {
                        return next_ping_;
                }
                const boost::posix_time::ptime& NextPong() const {
                        return next_pong_;
                }
                void SetNextPing (const boost::posix_time::ptime& time_) {
                        next_ping_ = time_;
                }
                void SetNextPong (const boost::posix_time::ptime& time_) {
                        next_pong_ = time_;
                }
                void IncrementPendingCount() {
                        pending_count_++;
                }
                void ClearPendingCount() {
                        pending_count_ = 0;
                }
                unsigned GetPendingCount() const {
                        return pending_count_;
                }

		const config_t& config_;

/* UPA context. */
		std::shared_ptr<upa_t> upa_;
/* This flag is set to false when Run should return. */
		boost::atomic_bool keep_running_;

		int in_nfds_, out_nfds_;
		fd_set in_rfds_, in_wfds_, in_efds_;
		fd_set out_rfds_, out_wfds_, out_efds_;
		struct timeval in_tv_, out_tv_;

// The time at which we should call DoDelayedWork.
		std::chrono::steady_clock::time_point delayed_work_time_;
                
// ... write end; ScheduleWork() writes a single byte to it
		net::SocketDescriptor wakeup_pipe_in_;
// ... read end; OnWakeup reads it and then breaks Run() out of its sleep
		net::SocketDescriptor wakeup_pipe_out_;

/* UPA client connection */
		RsslChannel* connection_;
		std::string component_text_;	/* API or TREP component name and version */
		std::string app_text_;		/* App name */
/* unique id per connection. */
		std::string prefix_;
/* flag that is false until permission is granted to submit data. */
                bool is_muted_;

/* Directory mapped ServiceID */
		boost::atomic_uint16_t service_id_;
/* Pending messages to flush. */
                unsigned pending_count_;
/* Field dictionary for caching */
		RsslDataDictionary rdm_dictionary_;
/* Watchlist of all items. */
                std::list<std::weak_ptr<item_stream_t>> directory_;
		boost::unordered_map<int32_t, std::weak_ptr<item_stream_t>> tokens_;
/* Last value cache. */
		RsslPayloadCacheHandle cache_handle_;
/* Response monitoring for tokens. */
		unsigned refresh_count_;
		bool in_sync_;
		bool pending_trigger_;
		Delegate* delegate_;
/* Item requests may appear before login success has been granted.  */
                bool is_logged_in_;
		int32_t token_;		/* incrementing unique id for streams */
                int32_t directory_token_;
                int32_t dictionary_token_;
                int32_t login_token_;	/* should always be 1 */
/* RSSL keepalive state. */
                boost::posix_time::ptime next_ping_;
                boost::posix_time::ptime next_pong_;
                unsigned ping_interval_;

/** Performance Counters **/
		boost::posix_time::ptime creation_time_, last_activity_;
		uint32_t cumulative_stats_[CONSUMER_PC_MAX];
		uint32_t snap_stats_[CONSUMER_PC_MAX];

		chromium::debug::LeakTracker<consumer_t> leak_tracker_;
	};

} /* namespace kinkan */

#endif /* CONSUMER_HH_ */

/* eof */
