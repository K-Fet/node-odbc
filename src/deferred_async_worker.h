/*
  Copyright (c) 2013, Dan VerWeire<dverweire@gmail.com>
  Copyright (c) 2010, Lee Smith<notwink@gmail.com>

  Permission to use, copy, modify, and/or distribute this software for any
  purpose with or without fee is hereby granted, provided that the above
  copyright notice and this permission notice appear in all copies.

  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

#ifndef _SRC_DEFERRED_ASYNC_WORKER_H
#define _SRC_DEFERRED_ASYNC_WORKER_H

#include "declarations.h"

class DeferredAsyncWorker : public Napi::AsyncWorker {
public:
    DeferredAsyncWorker(Napi::Promise::Deferred deferred);

    ~DeferredAsyncWorker();

    virtual void Execute() = 0;

    virtual void OnOK();

    virtual void OnError(const Napi::Error &e);

  protected:
    void Resolve(napi_value value);
    void Reject(napi_value value);

    Napi::Promise::Deferred deferred;
};

#endif
