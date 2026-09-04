// @ts-nocheck
// VLESS over WebSocket proxy for Deno Deploy (single-file, zero-dependency)
// Set env UUID, otherwise default is used.
// Entrypoint must use Deno.serve(handler) with NO explicit port on Deno Deploy.

const userID = Deno.env.get('UUID') || '7f3a9c2e-4d6b-4a8f-9c1e-2b5d8f0a3c67';
const isValidUser = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
  userID
);
if (!isValidUser) {
  console.log('not set valid UUID');
}

const HTML_401 = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>401 - UUID Not Valid</title>
</head>
<body>
    <h1 style="color: red;">Not set valid UUID in Environment Variables.</h1>
    <h2>Please use tool to generate and <span style="color: red;">remember</span> UUID or use this one <span
            style="color: blue;" id="uuidSpan"></span>
    </h2>
    <h3> You must use same UUID for login this page after config valid UUID Environment Variables
    </h3>
    <h2>Please refer to <a
            href="https://github.com/zizifn/edgetunnel/blob/main/doc/edge-tunnel-deno.md#%E6%B5%81%E7%A8%8B%E6%BC%94%E7%A4%BA">deno
            deploy guide</a>
    </h2>
    <script>
        let uuid = URL.createObjectURL(new Blob([])).substr(-36);
        document.getElementById('uuidSpan').textContent = uuid
    </script>
</body>
</html>`;

// 16 raw bytes -> RFC4122 uuid string (same as uuid.stringify)
function uuidFromBytes(bytes: Uint8Array): string {
  const hex: string[] = [];
  bytes.forEach((b) => hex.push(b.toString(16).padStart(2, '0')));
  return [
    hex.slice(0, 4).join(''),
    hex.slice(4, 6).join(''),
    hex.slice(6, 8).join(''),
    hex.slice(8, 10).join(''),
    hex.slice(10, 16).join(''),
  ].join('-');
}

// 16 bytes ipv6 -> "xxxx.xxxx.xxxx.xxxx.xxxx.xxxx.xxxx.xxxx" (same as lodash chunk map)
function ipv6FromBytes(bytes: Uint8Array): string {
  const parts: string[] = [];
  for (let i = 0; i < bytes.length; i += 2) {
    const hi = bytes[i].toString(16).padStart(2, '0');
    const lo = bytes[i + 1].toString(16).padStart(2, '0');
    parts.push(hi + lo);
  }
  return parts.join('.');
}

const handler = async (req: Request): Promise<Response> => {
  if (!isValidUser) {
    return new Response(HTML_401, {
      status: 401,
      headers: {
        'content-type': 'text/html; charset=utf-8',
      },
    });
  }

  const upgrade = req.headers.get('upgrade') || '';
  if (upgrade.toLowerCase() !== 'websocket') {
    // Non-WS request: simple liveness probe, no static UI bundled.
    const pathname = new URL(req.url).pathname;
    if (pathname === '/' || pathname === '') {
      return new Response('proxy is running...', {
        status: 200,
        headers: { 'content-type': 'text/plain; charset=utf-8' },
      });
    }
    return new Response('Not Found', { status: 404 });
  }

  const { socket, response } = Deno.upgradeWebSocket(req);
  let remoteConnection: Deno.TcpConn;
  let address = '';
  let port = 0;
  socket.onopen = () => console.log('socket opened');
  socket.onmessage = async (e) => {
    try {
      if (!(e.data instanceof ArrayBuffer)) {
        return;
      }
      const vlessBuffer: ArrayBuffer = e.data;

      if (remoteConnection) {
        const number = await remoteConnection.write(
          new Uint8Array(vlessBuffer)
        );
      } else {
        // VLESS header parse
        if (vlessBuffer.byteLength < 24) {
          console.log('invalid data');
          return;
        }
        const version = new Uint8Array(vlessBuffer.slice(0, 1));
        const isVaildUser =
          uuidFromBytes(new Uint8Array(vlessBuffer.slice(1, 17))) === userID;
        if (!isVaildUser) {
          console.log('in valid user');
          return;
        }

        const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];

        const command = new Uint8Array(
          vlessBuffer.slice(18 + optLength, 18 + optLength + 1)
        )[0];
        // 0x01 TCP
        // 0x02 UDP
        // 0x03 MUX
        if (command === 1) {
          // TCP supported
        } else {
          console.log(
            `command ${command} is not support, command 01-tcp,02-udp,03-mux`
          );
          socket.close();
          return;
        }
        const portIndex = 18 + optLength + 1;
        const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
        // port is big-Endian in raw data etc 80 == 0x005d
        const portRemote = new DataView(portBuffer).getInt16(0);
        port = portRemote;
        let addressIndex = portIndex + 2;
        const addressBuffer = new Uint8Array(
          vlessBuffer.slice(addressIndex, addressIndex + 1)
        );

        // 1--> ipv4  addressLength =4
        // 2--> domain name addressLength=addressBuffer[1]
        // 3--> ipv6  addressLength =16
        const addressType = addressBuffer[0];
        let addressLength = 0;
        let addressValueIndex = addressIndex + 1;
        let addressValue = '';
        switch (addressType) {
          case 1:
            addressLength = 4;
            addressValue = new Uint8Array(
              vlessBuffer.slice(
                addressValueIndex,
                addressValueIndex + addressLength
              )
            ).join('.');
            break;
          case 2:
            addressLength = new Uint8Array(
              vlessBuffer.slice(addressValueIndex, addressValueIndex + 1)
            )[0];
            addressValueIndex += 1;
            addressValue = new TextDecoder().decode(
              vlessBuffer.slice(
                addressValueIndex,
                addressValueIndex + addressLength
              )
            );
            break;
          case 3:
            addressLength = 16;
            addressValue = ipv6FromBytes(
              new Uint8Array(
                vlessBuffer.slice(
                  addressValueIndex,
                  addressValueIndex + addressLength
                )
              )
            );
            break;
          default:
            console.log(`[${address}:${port}] invild address`);
        }
        address = addressValue;
        if (!addressValue) {
          console.log(`[${address}:${port}] addressValue is empty`);
          socket.close();
          return;
        }
        console.log(`[${address}:${port}] connecting`);
        remoteConnection = await Deno.connect({
          port: port,
          hostname: addressValue,
        });

        const rawDataIndex = addressValueIndex + addressLength;
        const rawClientData = vlessBuffer.slice(rawDataIndex);
        await remoteConnection.write(new Uint8Array(rawClientData));

        let chunkDatas = [new Uint8Array([version[0], 0])];
        remoteConnection.readable
          .pipeTo(
            new WritableStream({
              start() {
                socket.send(new Blob(chunkDatas));
              },
              write(chunk, controller) {
                socket.send(new Blob([chunk]));
              },
            })
          )
          .catch((error) => {
            console.log(
              `[${address}:${port}] remoteConnection pipe to has error`,
              error
            );
          });
      }
    } catch (error) {
      console.log(`[${address}:${port}] request hadler has error`, error);
    }
  };
  socket.onerror = (e) =>
    console.log(`[${address}:${port}] socket errored:`, e);
  socket.onclose = () => console.log(`[${address}:${port}] socket closed`);
  return response;
};

// Deno Deploy: serve without explicit port/hostname so the platform can bind it.
Deno.serve(handler);
