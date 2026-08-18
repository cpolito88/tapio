# API reference

Generated from the source, so it cannot drift from what is installed.

## System

::: tapio.actor.system.ActorSystem

## Behaviors

::: tapio.actor.behavior.Behaviors

::: tapio.actor.behavior.AbstractBehavior

::: tapio.actor.behavior.Behavior

::: tapio.actor.behavior.Directive

::: tapio.actor.behavior.directive_of

## Context and refs

::: tapio.actor.context.ActorContext

::: tapio.actor.ref.ActorRef

::: tapio.actor.path.ActorPath

## Ask

::: tapio.actor.ask.ask

::: tapio.actor.ask.ask_through

::: tapio.actor.ask.PromiseRef

## Timers and stash

::: tapio.actor.timers.TimerScheduler

::: tapio.actor.stash.StashBuffer

## Message adapters

::: tapio.actor.adapter.AdapterRef

## Routers

::: tapio.actor.router.Routers

::: tapio.actor.router.RoutingStrategy

::: tapio.actor.router.RoundRobin

## Blocking calls

::: tapio.dispatch.blocking

## Messages and validation

::: tapio.message.Message

::: tapio.settings.TapioSettings

## Addressing and the wire format

::: tapio.remote.address.Address

::: tapio.remote.address.format_ref

::: tapio.remote.address.parse_ref

::: tapio.remote.registry

::: tapio.remote.codec

::: tapio.remote.context

::: tapio.settings.RemoteSettings

## Links and associations

::: tapio.remote.transport

::: tapio.remote.handshake

::: tapio.remote.association

::: tapio.remote.endpoint.RemoteEndpoint

::: tapio.remote.ref.RemoteRef

::: tapio.remote.ref.PeerWatch

::: tapio.settings.TLSSettings

::: tapio.remote.protocol

## Clustering

::: tapio.cluster.cluster.Cluster

::: tapio.cluster.member.Member

::: tapio.cluster.member.MemberStatus

::: tapio.cluster.gossip.Gossip

::: tapio.cluster.gossip.leader_actions

::: tapio.cluster.clock.VectorClock

::: tapio.cluster.clock.Ordering

::: tapio.cluster.reachability.Reachability

::: tapio.cluster.reachability.ReachabilityRecord

::: tapio.cluster.reachability.ReachabilityStatus

::: tapio.cluster.monitor.RingMonitor

::: tapio.cluster.monitor.monitored_by

::: tapio.cluster.downing.DownStrategy

::: tapio.cluster.downing.DownAll

::: tapio.cluster.downing.KeepMajority

::: tapio.cluster.downing.StaticQuorum

::: tapio.cluster.downing.KeepOldest

::: tapio.cluster.downing.LeaseMajority

::: tapio.cluster.downing.Lease

::: tapio.cluster.downing.LocalLease

::: tapio.cluster.messages

::: tapio.cluster.daemon.ClusterDaemon

::: tapio.settings.ClusterSettings

## Remote spawning

::: tapio.remote.spawner.remote_behavior

::: tapio.remote.spawner.spawner

::: tapio.remote.spawner.Spawn

::: tapio.remote.spawner.SpawnReply

::: tapio.remote.spawner.Spawned

::: tapio.remote.spawner.SpawnFailed

::: tapio.remote.spawner.SpawnFailure

::: tapio.remote.spawner.NoArgs

::: tapio.remote.spawner.RemoteFactory

::: tapio.remote.spawner.factory_for_key

## Reachability

::: tapio.remote.failure

::: tapio.remote.peers

::: tapio.actor.events.EventStream

::: tapio.actor.events.Subscription

## Supervision

::: tapio.actor.supervision.SupervisorStrategy

::: tapio.actor.supervision.Decision

::: tapio.actor.supervision.Backoff

::: tapio.actor.behavior.Supervise

## Mailbox and signals

::: tapio.actor.mailbox.Mailbox

::: tapio.actor.signals.PostStop

::: tapio.actor.signals.PreRestart

::: tapio.actor.signals.Terminated

::: tapio.actor.watch

::: tapio.actor.signals.ChildFailed

## Test support

::: tapio.testkit.probe.TestProbe

::: tapio.testkit.behavior.BehaviorTestKit

::: tapio.testkit.behavior.Spawned

::: tapio.testkit.behavior.Watched

::: tapio.testkit.behavior.RecordingRef

::: tapio.testkit.plugin

::: tapio.testkit.leaks

::: tapio.testkit.remote

## Errors

::: tapio.errors
