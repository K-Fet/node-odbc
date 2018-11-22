#include "deferred_async_worker.h"
#include "utils.h"

DeferredAsyncWorker::DeferredAsyncWorker(Napi::Promise::Deferred deferred)
    : Napi::AsyncWorker(Napi::Function::New(Env(), EmptyCallback)), deferred(deferred) {}

DeferredAsyncWorker::~DeferredAsyncWorker() {}

void DeferredAsyncWorker::OnOK() {
    Napi::Env env = Env();
    Napi::HandleScope scope(env);

    Resolve(env.Undefined());
};

void DeferredAsyncWorker::OnError(const Napi::Error &e) {
    Napi::Env env = Env();
    Napi::HandleScope scope(env);

    Reject(e.Value());
}

void DeferredAsyncWorker::Resolve(napi_value value) {
    deferred.Resolve(value);
    Callback().Call({});
}

void DeferredAsyncWorker::Reject(napi_value value) {
    deferred.Reject(value);
    Callback().Call({});
}

