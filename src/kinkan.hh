/* UPA interactive fake snapshot provider.
 *
 * An interactive provider sits listening on a port for RSSL connections,
 * once a client is connected requests may be submitted for snapshots or
 * subscriptions to item streams.  This application will broadcast updates
 * continuously independent of client interest and the provider side will
 * perform fan-out as required.
 *
 * The provider is not required to perform last value caching, forcing the
 * client to wait for a subsequent broadcast to actually see data.
 */

#ifndef KINKAN_HH_
#define KINKAN_HH_

#include <atomic>
#include <cstdint>
#include <memory>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "chromium/string_piece.hh"
#include "client.hh"
#include "consumer.hh"
#include "provider.hh"
#include "config.hh"

/* Maximum encoded size of an RSSL provider to client message. */
#define MAX_MSG_SIZE 4096

namespace kinkan
{
	class consumer_t;
	class provider_t;
	class upa_t;

/* Basic example structure for application state of an item stream. */
        class subscription_stream_t : public item_stream_t
        {
        public:
                explicit subscription_stream_t ()
			: snapshot_handle (0),
			  cow_handle (nullptr),
			  request_received (0)
                {
                }

/* Payload snapshot. */
		std::atomic<std::uintptr_t> snapshot_handle;
/* Cache entry for when source entry has updated. */
		RsslPayloadEntryHandle cow_handle;

/* Performance counters */
		uint32_t request_received;
        };

	class kinkan_t
/* Permit global weak pointer to application instance for shutdown notification. */
		: public std::enable_shared_from_this<kinkan_t>
		, public client_t::Delegate	/* Rssl requests */
		, public consumer_t::Delegate	/* Service status */
	{
	public:
		explicit kinkan_t();
		virtual ~kinkan_t();

/* Run as an application.  Blocks until Quit is called.  Returns the error code
 * Returns the error code to be returned by main().
 */
		int Run();
/* Quit an earlier call to Run(). */
		void Quit();

		virtual bool OnSync() override;
		virtual bool OnTrigger() override;
		virtual bool OnWrite (std::shared_ptr<item_stream_t> item_stream, const uint8_t rwf_major_version, const uint8_t rwf_minor_version, RsslMsg* msg) override;
		virtual bool OnRequest (uintptr_t handle, uint16_t rwf_version, int32_t token, uint16_t service_id, const std::string& item_name, bool use_attribinfo_in_updates) override;

		bool Initialize();
		void Reset();

	private:
/* Run core event loop. */
		void ConsumerLoop();
		void ProviderLoop();

/* Start the encapsulated provider instance until Stop is called.  Stop may be
 * called to pre-emptively prevent execution.
 */
		bool Start();
		void Stop();

		bool CheckTrigger (const uint8_t rwf_major_version, const uint8_t rwf_minor_version, RsslMsg* msg);
		bool WriteRaw (uint16_t rwf_version, int32_t token, uint16_t service_id, const chromium::StringPiece& item_name, const chromium::StringPiece& dacs_lock, RsslPayloadEntryHandle handle, void* data, size_t* length);

/* Mainloop procesing threads. */
		std::unique_ptr<boost::thread> consumer_thread_, provider_thread_;

/* Asynchronous shutdown notification mechanism. */
		boost::condition_variable consumer_cond_, provider_cond_;
		boost::mutex consumer_lock_, provider_lock_;
		bool consumer_shutdown_, provider_shutdown_;
/* Flag to indicate Stop has be called and thus prohibit start of new provider. */
		boost::atomic_bool shutting_down_;
/* Application configuration. */
		config_t config_;
/* UPA context. */
		std::shared_ptr<upa_t> upa_;
/* UPA provider */
		std::shared_ptr<provider_t> provider_;	
/* UPA consumer */
		std::shared_ptr<consumer_t> consumer_;	
/* Item stream. */
                boost::unordered_map<std::string, std::shared_ptr<subscription_stream_t>> streams_;
/* As worker state: */
/* Rssl message buffers */
		char provider_rssl_buf_[MAX_MSG_SIZE], consumer_rssl_buf_[MAX_MSG_SIZE];
		size_t provider_rssl_length_, consumer_rssl_length_;
	};

} /* namespace kinkan */

#endif /* KINKAN_HH_ */

/* eof */
