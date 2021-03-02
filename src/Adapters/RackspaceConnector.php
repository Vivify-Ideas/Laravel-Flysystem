<?php

declare(strict_types=1);

/*
 * This file is part of Laravel Flysystem.
 *
 * (c) Graham Campbell <graham@alt-three.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GrahamCampbell\Flysystem\Adapters;

use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GrahamCampbell\Manager\ConnectorInterface;
use Illuminate\Support\Arr;
use InvalidArgumentException;
use League\Flysystem\Rackspace\RackspaceAdapter;
use OpenStack\Common\Transport\Utils as TransportUtils;
use OpenStack\Identity\v2\Service;
use OpenStack\ObjectStore\v1\Models\Container;
use OpenStack\OpenStack;

/**
 * This is the rackspace connector class.
 *
 * @author Graham Campbell <graham@alt-three.com>
 */
class RackspaceConnector implements ConnectorInterface
{
    /**
     * Establish an adapter connection.
     *
     * @param string[] $config
     *
     * @throws \InvalidArgumentException
     *
     * @return \League\Flysystem\Rackspace\RackspaceAdapter
     */
    public function connect(array $config)
    {
        $auth = $this->getAuth($config);
        $client = $this->getClient($auth);

        return $this->getAdapter($client);
    }

    /**
     * Get the authentication data.
     *
     * @param string[] $config
     *
     * @throws \InvalidArgumentException
     *
     * @return string[]
     */
    protected function getAuth(array $config)
    {
        if (!array_key_exists('username', $config) || !array_key_exists('password', $config)) {
            throw new InvalidArgumentException('The rackspace connector requires authentication.');
        }
        
        if (!array_key_exists('tenantId', $config)) {
            throw new InvalidArgumentException('The rackspace connector requires tenant ID.');
        }

        if (!array_key_exists('endpoint', $config)) {
            throw new InvalidArgumentException('The rackspace connector requires endpoint configuration.');
        }

        if (!array_key_exists('region', $config)) {
            throw new InvalidArgumentException('The rackspace connector requires region configuration.');
        }

        if (!array_key_exists('container', $config)) {
            throw new InvalidArgumentException('The rackspace connector requires container configuration.');
        }

        return Arr::only($config, ['username', 'password', 'tenantId', 'endpoint', 'region', 'container']);
    }

    /**
     * Get the rackspace client.
     *
     * @param string[] $auth
     *
     * @return \OpenStack\ObjectStore\v1\Models\Container
     */
    protected function getClient(array $auth)
    {
        $httpClient = new Client([
            'base_uri' => TransportUtils::normalizeUrl($auth['endpoint']),
            'handler'  => HandlerStack::create(),
        ]);

        $options = [
            'authUrl'         => $auth['endpoint'],
            'region'          => $auth['region'],
            'username'        => $auth['username'],
            'password'        => $auth['password'],
            'tenantId'        => $auth['tenantId'],
            'identityService' => Service::factory($httpClient),
        ];

        $client = new OpenStack($options);

        $objectStoreOptions = ['catalogName' => 'cloudFiles'];

        return $client->objectStoreV1($objectStoreOptions)->getContainer($auth['container']);
    }

    /**
     * Get the rackspace adapter.
     *
     * @param \OpenStack\ObjectStore\v1\Models\Container $client
     *
     * @return \League\Flysystem\Rackspace\RackspaceAdapter
     */
    protected function getAdapter(Container $client)
    {
        return new RackspaceAdapter($client);
    }
}
